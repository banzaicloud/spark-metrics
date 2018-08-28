package com.banzaicloud.spark.metrics

import java.util

import com.codahale.metrics.MetricRegistry
import io.prometheus.client.Collector
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.dropwizard.DropwizardExports

import scala.util.matching.Regex
import collection.JavaConverters._

/**
  * Exporter that converts DropWizard type metrics to Prometheus metrics while applies a regexp to change the name of
  * the metrics
  * @param metricsNameCaptureRegex the regexp to capture metrics name parts to replace, e.g. (\w+)-(\w+)
  * @param replacement the replacement string to replace the captured part of the metrics name with, e.g. ${1}/${2}
  * @param registry the metrics registry that holds all the DropWizard metrics
  */
class DropwizardExportsWithMetricNameCaptureAndReplace(
                                            metricsNameCaptureRegex: Regex,
                                            replacement: String,
                                            registry: MetricRegistry)
  extends DropwizardExports(registry) {

  /**
    * Returns the list of metrics stored by #registry in format expected by Prometheus. Applies #metricsNameCaptureRegex
    * regular expression to metric name to capture the parts that will be replaced by the expression specified
    * in #replacement
    * @return metrics collection with replaced names.
    */
  override def collect(): util.List[Collector.MetricFamilySamples] = {
    val metrics = super.collect().asScala

    metrics map {
      mfs => new Collector.MetricFamilySamples(
        metricsNameCaptureRegex.replaceAllIn(mfs.name, replacement),
        mfs.`type`,
        mfs.help,
        mfs.samples.asScala map {
          s => new MetricFamilySamples.Sample(
            metricsNameCaptureRegex.replaceAllIn(s.name, replacement),
            s.labelNames,
            s.labelValues,
            s.value,
            s.timestampMs
          )
        } asJava
      )
    } asJava
  }
}
