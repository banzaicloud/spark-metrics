package com.banzaicloud.spark.metrics

import java.util

import com.banzaicloud.spark.metrics.PushTimestampDecorator.PushTimestampProvider
import com.codahale.metrics.MetricRegistry
import io.prometheus.client.Collector
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.Collector.MetricFamilySamples.Sample
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.jmx.JmxCollector
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import scala.util.matching.Regex

object NameDecorator {
  case class Replace(Regex: Regex, replacement: String)
}

trait NameDecorator extends CollectorDecorator {
  val metricsNameReplace: Option[NameDecorator.Replace]

  protected override def familyName(familySamples: Collector.MetricFamilySamples): String = {
    replaceName(super.familyName(familySamples))
  }
  protected override def sampleName(sample: Sample): String = {
    replaceName(super.sampleName(sample))
  }

  private def replaceName(name: String) = {
    metricsNameReplace.map {
      case NameDecorator.Replace(regex, replacement) => regex.replaceAllIn(name, replacement)
    }.getOrElse(name)
  }
}

trait LabelsDecorator extends CollectorDecorator {
    val extraLabels: Map[String, String]

    private val labelNames = extraLabels.keys.toList.asJava
    private val labelValues = extraLabels.values.toList.asJava

    override protected def sampleLabelNames(sample: Sample): util.List[String] = {
      mergeLists(super.sampleLabelNames(sample), labelNames)
    }

    override protected def sampleLabelValues(sample: Sample): util.List[String] = {
      mergeLists(super.sampleLabelValues(sample), labelValues)
    }

    private def mergeLists(list1: util.List[String], list2: util.List[String]): util.List[String] = {
      val newList = new util.ArrayList[String](list1)
      newList.addAll(list2)
      newList
    }
}

object PushTimestampDecorator {
  case class PushTimestampProvider(getTimestamp: () => Long = System.currentTimeMillis) extends AnyVal
}
trait PushTimestampDecorator extends CollectorDecorator {
  val maybeTimestampProvider: Option[PushTimestampProvider]

  override protected def sampleTimestamp(sample: Sample): java.lang.Long = {
    maybeTimestampProvider match {
      case Some(t) => t.getTimestamp()
      case None => sample.timestampMs
    }
  }
}

trait ConstantHelpDecorator extends CollectorDecorator {
  val constatntHelp: String

  override protected def helpMessage(familySamples: MetricFamilySamples): String = {
    constatntHelp
  }
}

trait DeduplicatorDecorator extends CollectorDecorator with Logging {

  override def collect(): util.List[Collector.MetricFamilySamples] = {
    val metrics = super.collect().asScala
    metrics
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
  }
}

class SparkDropwizardExports(private val registry: MetricRegistry,
                             override val metricsNameReplace: Option[NameDecorator.Replace],
                             override val extraLabels: Map[String, String],
                             override val maybeTimestampProvider: Option[PushTimestampProvider])
  extends CollectorDecorator(new DropwizardExports(registry))
    with NameDecorator
    with LabelsDecorator
    with PushTimestampDecorator
    with ConstantHelpDecorator
    with DeduplicatorDecorator {
  override val constatntHelp: String = "Generated from Dropwizard metric import"
}

class SparkJmxExports(private val jmxCollector: JmxCollector,
                 override val extraLabels: Map[String, String],
                 override val maybeTimestampProvider: Option[PushTimestampProvider])
  extends CollectorDecorator(jmxCollector)
    with LabelsDecorator
    with PushTimestampDecorator
    with DeduplicatorDecorator