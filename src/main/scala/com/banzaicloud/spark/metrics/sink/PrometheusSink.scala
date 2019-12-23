/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.banzaicloud.spark.metrics.sink

import java.io.File
import java.net.InetAddress
import java.net.URI
import java.net.UnknownHostException
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.banzaicloud.metrics.prometheus.client.exporter.PushGatewayWithTimestamp
import com.banzaicloud.spark.metrics.DropwizardExportsWithMetricNameTransform
import com.codahale.metrics._
import io.prometheus.client.{Collector, CollectorRegistry}
import io.prometheus.client.dropwizard.DropwizardExports
import org.apache.spark.internal.Logging
import io.prometheus.jmx.JmxCollector
import org.apache.spark.{SparkConf, SparkEnv}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex
import PrometheusSink._

object PrometheusSink {

  val DEFAULT_PUSH_PERIOD: Int = 10
  val DEFAULT_PUSH_PERIOD_UNIT: TimeUnit = TimeUnit.SECONDS
  val DEFAULT_PUSHGATEWAY_ADDRESS: String = "127.0.0.1:9091"
  val DEFAULT_PUSHGATEWAY_ADDRESS_PROTOCOL: String = "http"
  val PUSHGATEWAY_ENABLE_TIMESTAMP: Boolean = false

  val KEY_PUSH_PERIOD = "period"
  val KEY_PUSH_PERIOD_UNIT = "unit"
  val KEY_PUSHGATEWAY_ADDRESS = "pushgateway-address"
  val KEY_PUSHGATEWAY_ADDRESS_PROTOCOL = "pushgateway-address-protocol"
  val KEY_PUSHGATEWAY_ENABLE_TIMESTAMP = "pushgateway-enable-timestamp"
  val DEFAULT_KEY_JMX_COLLECTOR_CONFIG = "/opt/spark/conf/jmx_collector.yaml"

  // metrics name replacement
  val KEY_METRICS_NAME_CAPTURE_REGEX = "metrics-name-capture-regex"
  val KEY_METRICS_NAME_REPLACEMENT = "metrics-name-replacement"
  val KEY_METRICS_NAME_TO_LOWERCASE = "metrics-name-to-lowercase"

  val SPARK_METRIC_LABELS_CONF = "spark.SPARK_METRIC_LABELS"

  val KEY_ENABLE_DROPWIZARD_COLLECTOR = "enable-dropwizard-collector"
  val KEY_ENABLE_JMX_COLLECTOR = "enable-jmx-collector"
  val KEY_ENABLE_HOSTNAME_IN_INSTANCE = "enable-hostname-in-instance"
  val KEY_JMX_COLLECTOR_CONFIG = "jmx-collector-config"

  // labels
  val KEY_LABELS = "labels"

}

abstract class PrometheusSink(property: Properties, registry: MetricRegistry)
    extends Logging {

  private val lbv = raw"(.+)\s*=\s*(.*)".r

  protected class Reporter(registry: MetricRegistry)
      extends ScheduledReporter(registry,
                                "prometheus-reporter",
                                MetricFilter.ALL,
                                TimeUnit.SECONDS,
                                TimeUnit.MILLISECONDS) {

    val defaultSparkConf: SparkConf = new SparkConf(true)

    @throws(classOf[UnknownHostException])
    override def report(gauges: util.SortedMap[String, Gauge[_]],
                        counters: util.SortedMap[String, Counter],
                        histograms: util.SortedMap[String, Histogram],
                        meters: util.SortedMap[String, Meter],
                        timers: util.SortedMap[String, Timer]): Unit = {

      // SparkEnv may become available only after metrics sink creation thus retrieving
      // SparkConf from spark env here and not during the creation/initialisation of PrometheusSink.
      val sparkConf: SparkConf =
        Option(SparkEnv.get).map(_.conf).getOrElse(defaultSparkConf)

      val metricsNamespace: Option[String] =
        sparkConf.getOption("spark.metrics.namespace")
      val sparkAppId: Option[String] = sparkConf.getOption("spark.app.id")
      val sparkAppName: Option[String] = sparkConf.getOption("spark.app.name")
      val executorId: Option[String] = sparkConf.getOption("spark.executor.id")

      logInfo(
        s"metricsNamespace=$metricsNamespace, sparkAppName=$sparkAppName, sparkAppId=$sparkAppId, " +
          s"executorId=$executorId")

      val labelsMap: Option[Map[String, String]] = collectLabels(sparkConf)
      logInfo(s"$KEY_LABELS -> ${labelsMap.getOrElse("")}")
      logInfo(s"$SPARK_METRIC_LABELS_CONF -> ${labelsMap.getOrElse("")}")

      val role: String = (sparkAppId, executorId) match {
        case (Some(_), Some("driver")) | (Some(_), Some("<driver>")) => "driver"
        case (Some(_), Some(_))                                      => "executor"
        case _                                                       => "unknown"
      }

      val job: String = role match {
        case "driver"   => metricsNamespace.getOrElse(sparkAppId.get)
        case "executor" => metricsNamespace.getOrElse(sparkAppId.get)
        case _          => metricsNamespace.getOrElse("unknown")
      }

      val instance: String =
        if (enableHostNameInInstance) InetAddress.getLocalHost.getHostName
        else sparkAppId.getOrElse("")

      val appName: String = sparkAppName.getOrElse("")

      logInfo(s"role=$role, job=$job")

      val groupingKey: Map[String, String] = (role, executorId) match {
        case ("driver", _) =>
          labelsMap match {
            case Some(m) =>
              Map("role" -> role, "app_name" -> appName, "instance" -> instance) ++ m
            case _ =>
              Map("role" -> role, "app_name" -> appName, "instance" -> instance)
          }

        case ("executor", Some(id)) =>
          labelsMap match {
            case Some(m) =>
              Map("role" -> role,
                  "number" -> id,
                  "app_name" -> appName,
                  "instance" -> instance) ++ m
            case _ =>
              Map("role" -> role,
                  "number" -> id,
                  "app_name" -> appName,
                  "instance" -> instance)
          }

        case _ => Map("role" -> role)
      }

      val metricTimestamp =
        if (enableTimestamp) Some(s"${System.currentTimeMillis}") else None

      pushGateway.pushAdd(pushRegistry,
                          job,
                          groupingKey.asJava,
                          metricTimestamp.orNull)
    }
  }

  val pollPeriod: Int =
    Option(property.getProperty(KEY_PUSH_PERIOD))
      .map(_.toInt)
      .getOrElse(DEFAULT_PUSH_PERIOD)

  val pollUnit: TimeUnit =
    Option(property.getProperty(KEY_PUSH_PERIOD_UNIT))
      .map { s =>
        TimeUnit.valueOf(s.toUpperCase)
      }
      .getOrElse(DEFAULT_PUSH_PERIOD_UNIT)

  val pushGatewayAddress =
    Option(property.getProperty(KEY_PUSHGATEWAY_ADDRESS))
      .getOrElse(DEFAULT_PUSHGATEWAY_ADDRESS)

  val pushGatewayAddressProtocol =
    Option(property.getProperty(KEY_PUSHGATEWAY_ADDRESS_PROTOCOL))
      .getOrElse(DEFAULT_PUSHGATEWAY_ADDRESS_PROTOCOL)

  val enableTimestamp: Boolean =
    Option(property.getProperty(KEY_PUSHGATEWAY_ENABLE_TIMESTAMP))
      .map(_.toBoolean)
      .getOrElse(PUSHGATEWAY_ENABLE_TIMESTAMP)

  val metricsNameCaptureRegex: Option[Regex] = {
    val regexExpr = property.getProperty(KEY_METRICS_NAME_CAPTURE_REGEX)
    Option(regexExpr).map(new Regex(_))
  }

  val metricsNameReplacement: String =
    Option(property.getProperty(KEY_METRICS_NAME_REPLACEMENT))
      .getOrElse("")

  val toLowerCase: Boolean =
    Option(property.getProperty(KEY_METRICS_NAME_TO_LOWERCASE))
      .map(_.toBoolean)
      .getOrElse(false)

  // validate pushgateway host:port
  Try(new URI(s"$pushGatewayAddressProtocol://$pushGatewayAddress")).get

  // validate metrics name capture regex
  if (metricsNameCaptureRegex.isDefined && metricsNameReplacement == "") {
    throw new IllegalArgumentException(
      "Metrics name replacement must be specified if metrics name capture regexp is set !")
  }

  val enableDropwizardCollector: Boolean =
    Option(property.getProperty(KEY_ENABLE_DROPWIZARD_COLLECTOR))
      .map(_.toBoolean)
      .getOrElse(true)
  val enableJmxCollector: Boolean =
    Option(property.getProperty(KEY_ENABLE_JMX_COLLECTOR))
      .map(_.toBoolean)
      .getOrElse(false)
  val enableHostNameInInstance: Boolean =
    Option(property.getProperty(KEY_ENABLE_HOSTNAME_IN_INSTANCE))
      .map(_.toBoolean)
      .getOrElse(false)
  val jmxCollectorConfig =
    Option(property.getProperty(KEY_JMX_COLLECTOR_CONFIG))
      .getOrElse(DEFAULT_KEY_JMX_COLLECTOR_CONFIG)

  checkMinimalPollingPeriod(pollUnit, pollPeriod)

  logInfo("Initializing Prometheus Sink...")
  logInfo(s"Metrics polling period -> $pollPeriod $pollUnit")
  logInfo(s"Metrics timestamp enabled -> $enableTimestamp")
  logInfo(s"$KEY_PUSHGATEWAY_ADDRESS -> $pushGatewayAddress")
  logInfo(s"$KEY_PUSHGATEWAY_ADDRESS_PROTOCOL -> $pushGatewayAddressProtocol")
  logInfo(
    s"$KEY_METRICS_NAME_CAPTURE_REGEX -> ${metricsNameCaptureRegex.getOrElse("")}")
  logInfo(s"$KEY_METRICS_NAME_REPLACEMENT -> $metricsNameReplacement")
  logInfo(s"$KEY_METRICS_NAME_TO_LOWERCASE -> $toLowerCase")

  logInfo(s"$KEY_JMX_COLLECTOR_CONFIG -> $jmxCollectorConfig")

  val pushRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

  lazy val sparkMetricExports: DropwizardExports =
    metricsNameCaptureRegex match {
      case Some(r) =>
        new DropwizardExportsWithMetricNameTransform(registry,
                                                     r,
                                                     metricsNameReplacement,
                                                     toLowerCase)
      case _ => new com.banzaicloud.spark.metrics.DropwizardExports(registry)
    }

  lazy val jmxMetrics: JmxCollector = new JmxCollector(
    new File(jmxCollectorConfig))

  val pushGateway: PushGatewayWithTimestamp =
    new PushGatewayWithTimestamp(
      s"$pushGatewayAddressProtocol://$pushGatewayAddress")

  val reporter = new Reporter(registry)

  def start(): Unit = {
    if (enableDropwizardCollector) {
      sparkMetricExports.register(pushRegistry)
    }
    if (enableJmxCollector) {
      jmxMetrics.register(pushRegistry)
    }
    reporter.start(pollPeriod, pollUnit)
  }

  def stop(): Unit = {
    reporter.stop()
    if (enableDropwizardCollector) {
      pushRegistry.unregister(sparkMetricExports)
    }
    if (enableJmxCollector) {
      pushRegistry.unregister(jmxMetrics)
    }
  }

  def report(): Unit = {
    reporter.report()
  }

  private def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int) {
    val period = TimeUnit.SECONDS.convert(pollPeriod, pollUnit)
    if (period < 1) {
      throw new IllegalArgumentException(
        s"Polling period $pollPeriod $pollUnit below than minimal polling period ")
    }
  }

  private def parseLabel(label: String): (String, String) = {
    label match {
      case lbv(label, value) => (Collector.sanitizeMetricName(label), value)
      case _ =>
        throw new IllegalArgumentException(
          "Can not parse labels ! Labels should be in label=value separated by commas format.")
    }
  }

  private def parseLabels(labels: String): Try[Map[String, String]] = {
    val kvs = labels.split(',')
    val parsedLabels = Try(kvs.map(parseLabel).toMap)

    parsedLabels
  }

  // an attempt to pass labels via env vars
  private def collectEnvLabels(sparkConf: SparkConf): Map[String, String] = {
    val kvs = sparkConf.get(SPARK_METRIC_LABELS_CONF, "")
    if (kvs.isEmpty) {
      return Map.empty
    }
    kvs.split(",").map(parseLabel).toMap
  }

  /**
    * 2 sources for labels:
    * 1. from config
    * @return
    */
  private def collectLabels(
      sparkConf: SparkConf): Option[Map[String, String]] = {
    // parse labels
    val configLabels: Option[Try[Map[String, String]]] =
      Option(property.getProperty(KEY_LABELS))
        .map(parseLabels)

    val configLabelsMap = configLabels match {
      case Some(Success(lm))  => lm
      case Some(Failure(err)) => throw err
      case _                  => Map.empty
    }

    val envLabels = collectEnvLabels(sparkConf)
    Option(configLabelsMap ++ envLabels)
  }
}
