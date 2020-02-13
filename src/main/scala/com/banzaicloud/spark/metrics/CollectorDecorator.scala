package com.banzaicloud.spark.metrics

import java.util

import com.banzaicloud.spark.metrics.CollectorDecorator.FamilyBuilder
import io.prometheus.client.Collector
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.Collector.MetricFamilySamples.Sample

import scala.collection.JavaConverters._

object CollectorDecorator {
  case class FamilyBuilder(familyName : MetricFamilySamples => String = _.name,
                           helpMessage : MetricFamilySamples => String = _.help,
                           sampleBuilder: SampleBuilder = SampleBuilder()) {
    def build(mfs: MetricFamilySamples): MetricFamilySamples = {
      new MetricFamilySamples(
        familyName(mfs),
        mfs.`type`,
        helpMessage(mfs),
        mfs.samples
          .asScala
          .map(sampleBuilder.build(_))
          .asJava
      )
    }
  }

  case class SampleBuilder(sampleName : Sample => String = _.name,
                           sampleLabelNames : Sample => util.List[String] = _.labelNames,
                           sampleLabelValues : Sample => util.List[String] = _.labelValues,
                           sampleTimestamp : Sample => java.lang.Long = _.timestampMs) {
    def build(sample: Sample): Sample  = {
      new Sample(
        sampleName(sample),
        sampleLabelNames(sample),
        sampleLabelValues(sample),
        sample.value,
        sampleTimestamp(sample)
      )
    }
  }

}

abstract class CollectorDecorator(parent: Collector)
  extends io.prometheus.client.Collector {


  override def collect(): util.List[MetricFamilySamples] = {
    map(parent.collect(), familyBuilder)
  }

  protected def familyBuilder: FamilyBuilder = FamilyBuilder()

  protected def map(source: util.List[MetricFamilySamples], builder: FamilyBuilder) = {
    source.asScala.toList.map(builder.build(_)).asJava
  }
}
