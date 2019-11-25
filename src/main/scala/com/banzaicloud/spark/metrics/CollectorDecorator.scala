package com.banzaicloud.spark.metrics

import java.util

import io.prometheus.client.Collector
import io.prometheus.client.Collector.{Describable, MetricFamilySamples}
import io.prometheus.client.Collector.MetricFamilySamples.Sample

import scala.collection.JavaConverters._

abstract class CollectorDecorator(parent: Collector)
  extends io.prometheus.client.Collector {


  override def collect(): util.List[Collector.MetricFamilySamples] = {
    val metrics = parent.collect().asScala

    metrics.map {
      mfs => new Collector.MetricFamilySamples(
        familyName(mfs),
        mfs.`type`,
        helpMessage(mfs),
        mfs.samples
          .asScala
          .map(sampleBuilder)
          .asJava
      )
    }.asJava
  }

  protected def sampleBuilder(sample: Sample): MetricFamilySamples.Sample  = {
    new MetricFamilySamples.Sample(
      sampleName(sample),
      sampleLabelNames(sample),
      sampleLabelValues(sample),
      sample.value,
      sampleTimestamp(sample)
    )
  }

  protected def helpMessage(familySamples: MetricFamilySamples): String = {
    familySamples.help
  }
  protected def familyName(familySamples: MetricFamilySamples): String = {
    familySamples.name
  }
  protected def sampleName(sample: Sample): String = {
    sample.name
  }
  protected def sampleLabelNames(sample: Sample): util.List[String] = {
    sample.labelNames
  }
  protected def sampleLabelValues(sample: Sample): util.List[String] = {
    sample.labelValues
  }
  protected def sampleTimestamp(sample: Sample): java.lang.Long = {
    sample.timestampMs
  }
}
