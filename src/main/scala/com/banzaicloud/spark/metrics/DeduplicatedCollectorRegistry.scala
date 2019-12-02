package com.banzaicloud.spark.metrics

import java.{lang, util}
import java.util.{Collections, Enumeration}

import io.prometheus.client.{Collector, CollectorRegistry}

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging

class DeduplicatedCollectorRegistry(parent: CollectorRegistry = CollectorRegistry.defaultRegistry)
  extends CollectorRegistry with Logging {
  private type MetricsEnum = util.Enumeration[Collector.MetricFamilySamples]

  override def register(m: Collector): Unit = parent.register(m)

  override def unregister(m: Collector): Unit = parent.unregister(m)

  override def clear(): Unit = parent.clear()

  override def getSampleValue(name: String, labelNames: Array[String], labelValues: Array[String]): lang.Double = {
    parent.getSampleValue(name, labelNames, labelValues)
  }

  override def getSampleValue(name: String): lang.Double = parent.getSampleValue(name)

  override def metricFamilySamples(): MetricsEnum = {
    deduplicate(parent.metricFamilySamples())
  }

  override def filteredMetricFamilySamples(includedNames: util.Set[String]): MetricsEnum = {
    deduplicate(parent.filteredMetricFamilySamples(includedNames))
  }

  private def deduplicate(source: MetricsEnum): MetricsEnum = {
    val metrics = source.asScala.toSeq
    val deduplicated = metrics
      .groupBy(f => (f.name, f.`type`))
      .flatMap {
        case (_, single) if single.lengthCompare(2) < 0 =>

          single
        case ((name, metricType), duplicates) =>
          logDebug(s"Found ${duplicates.length} metrics with the same name '${name}' and type ${metricType}")
          duplicates.lastOption
      }
      .toList
      .asJava
    Collections.enumeration(deduplicated)
  }
}
