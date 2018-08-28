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

import java.net.URI
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.banzaicloud.metrics.prometheus.client.exporter.PushGatewayWithTimestamp
import com.banzaicloud.spark.metrics.DropwizardExportsWithMetricNameCaptureAndReplace
import com.codahale.metrics._
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.dropwizard.DropwizardExports
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.METRICS_NAMESPACE
import org.apache.spark.metrics.sink.Sink
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.matching.Regex


class PrometheusSink(
                      val property: Properties,
                      val registry: MetricRegistry,
                      securityMgr: SecurityManager)
  extends Sink with Logging {

  protected class Reporter(registry: MetricRegistry)
    extends ScheduledReporter(
      registry,
      "prometheus-reporter",
      MetricFilter.ALL,
      TimeUnit.SECONDS,
      TimeUnit.MILLISECONDS) {

    val defaultSparkConf: SparkConf = new SparkConf(true)

    override def report(
                         gauges: util.SortedMap[String, Gauge[_]],
                         counters: util.SortedMap[String, Counter],
                         histograms: util.SortedMap[String, Histogram],
                         meters: util.SortedMap[String, Meter],
                         timers: util.SortedMap[String, Timer]): Unit = {

      // SparkEnv may become available only after metrics sink creation thus retrieving
      // SparkConf from spark env here and not during the creation/initialisation of PrometheusSink.
      val sparkConf: SparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(defaultSparkConf)


      val metricsNamespace: Option[String] = sparkConf.get(METRICS_NAMESPACE)
      val sparkAppId: Option[String] = sparkConf.getOption("spark.app.id")
      val executorId: Option[String] = sparkConf.getOption("spark.executor.id")

      logInfo(s"metricsNamespace=$metricsNamespace, sparkAppId=$sparkAppId, " +
        s"executorId=$executorId")

      val role: String = (sparkAppId, executorId) match {
        case (Some(_), Some("driver")) | (Some(_), Some("<driver>"))=> "driver"
        case (Some(_), Some(_)) => "executor"
        case _ => "shuffle"
      }

      val job: String = role match {
        case "driver" => metricsNamespace.getOrElse(sparkAppId.get)
        case "executor" => metricsNamespace.getOrElse(sparkAppId.get)
        case _ => metricsNamespace.getOrElse("shuffle")
      }

      val instance: String = sparkAppId.getOrElse("")

      logInfo(s"role=$role, job=$job")

      val groupingKey: Map[String, String] = (role, executorId) match {
        case ("driver", _) => Map("role" -> role, "instance" -> instance)
        case ("executor", Some(id)) => Map ("role" -> role, "number" -> id, "instance" -> instance)
        case _ => Map("role" -> role)
      }

      val metricTimestamp = if (enableTimestamp) Some(s"${System.currentTimeMillis}") else None

      pushGateway.pushAdd(pushRegistry, job, groupingKey.asJava, metricTimestamp.orNull)
    }
  }

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

  // metrics name replacement
  val KEY_METRICS_NAME_CAPTURE_REGEX = "metrics-name-capture-regex"
  val KEY_METRICS_NAME_REPLACEMENT = "metrics-name-replacement"


  val pollPeriod: Int =
    Option(property.getProperty(KEY_PUSH_PERIOD))
      .map(_.toInt)
      .getOrElse(DEFAULT_PUSH_PERIOD)

  val pollUnit: TimeUnit =
    Option(property.getProperty(KEY_PUSH_PERIOD_UNIT))
      .map { s => TimeUnit.valueOf(s.toUpperCase) }
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

  val metricsNameCaptureRegex: Option[Regex] =
    Option(property.getProperty(KEY_METRICS_NAME_CAPTURE_REGEX))
      .map(new Regex(_))

  val metricsNameReplacement: String =
    Option(property.getProperty(KEY_METRICS_NAME_REPLACEMENT))
        .getOrElse("")

  // validate pushgateway host:port
  Try(new URI(s"$pushGatewayAddressProtocol://$pushGatewayAddress")).get

  // validate metrics name capture regex
  if (metricsNameCaptureRegex.isDefined && metricsNameReplacement == "") {
    throw new IllegalArgumentException("Metrics name replacement must be specified if metrics name capture regexp is set !")
  }

  checkMinimalPollingPeriod(pollUnit, pollPeriod)

  logInfo("Initializing Prometheus Sink...")
  logInfo(s"Metrics polling period -> $pollPeriod $pollUnit")
  logInfo(s"Metrics timestamp enabled -> $enableTimestamp")
  logInfo(s"$KEY_PUSHGATEWAY_ADDRESS -> $pushGatewayAddress")
  logInfo(s"$KEY_PUSHGATEWAY_ADDRESS_PROTOCOL -> $pushGatewayAddressProtocol")
  logInfo(s"$KEY_METRICS_NAME_CAPTURE_REGEX -> ${metricsNameCaptureRegex.getOrElse("")}")
  logInfo(s"$KEY_METRICS_NAME_REPLACEMENT -> $metricsNameReplacement")

  val pushRegistry: CollectorRegistry = CollectorRegistry.defaultRegistry

  val sparkMetricExports: DropwizardExports = metricsNameCaptureRegex match {
    case Some(r) => new DropwizardExportsWithMetricNameCaptureAndReplace(r, metricsNameReplacement, registry)
    case _ => new DropwizardExports(registry)
  }

  val pushGateway: PushGatewayWithTimestamp =
    new PushGatewayWithTimestamp(s"$pushGatewayAddressProtocol://$pushGatewayAddress")

  val reporter = new Reporter(registry)

  override def start(): Unit = {
    sparkMetricExports.register(pushRegistry)
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop(): Unit = {
    reporter.stop()
    pushRegistry.unregister(sparkMetricExports)
  }

  override def report(): Unit = {
    reporter.report()
  }

  private def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int) {
    val period = TimeUnit.SECONDS.convert(pollPeriod, pollUnit)
    if (period < 1) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }
}
