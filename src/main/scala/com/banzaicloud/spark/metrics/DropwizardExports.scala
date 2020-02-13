package com.banzaicloud.spark.metrics

import java.util

import com.codahale.metrics.MetricRegistry
import io.prometheus.client.Collector
import io.prometheus.client.Collector.MetricFamilySamples

import collection.JavaConverters._
import scala.collection.mutable

/**
  * Exporter that overrides the help message generated by [[io.prometheus.client.dropwizard.DropwizardExports]]
  * such as help messages are consistent for the same metrics across multiple runs
  * @param registry the metrics registry that holds all the DropWizard metrics
  */
class DropwizardExports(registry: MetricRegistry)
    extends io.prometheus.client.dropwizard.DropwizardExports(registry) {

  override def collect(): util.List[Collector.MetricFamilySamples] = {
    val metrics = super.collect().asScala
    prepareMetrics(metrics)
  }

  def prepareMetrics(metrics: mutable.Buffer[MetricFamilySamples])
    : util.List[Collector.MetricFamilySamples] = {
    metrics map { mfs =>
      new Collector.MetricFamilySamples(
        transformMetricsName(mfs.name),
        mfs.`type`,
        "Generated from Dropwizard metric import",
        mfs.samples.asScala map { s =>
          new MetricFamilySamples.Sample(
            transformMetricsName(s.name),
            s.labelNames,
            s.labelValues,
            s.value,
            s.timestampMs
          )
        } asJava
      )
    } asJava
  }

  // no op
  def transformMetricsName(originMetricName: String): String = originMetricName
}