package com.banzaicloud.spark.metrics

import java.io.File
import java.util

import com.codahale.metrics.MetricRegistry
import io.prometheus.client.Collector
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.Collector.MetricFamilySamples.Sample
import io.prometheus.client.dropwizard.DropwizardExports
import io.prometheus.jmx.JmxCollector

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

trait ConstantTimestampDecorator extends CollectorDecorator {
  val maybeTimestamp: Option[Long]

  override protected def sampleTimestamp(sample: Sample): java.lang.Long = {
    maybeTimestamp match {
      case Some(t) => t
      case None => null
    }
  }
}
trait ConstantHelpDecorator extends CollectorDecorator {
  val constatntHelp: String

  override protected def helpMessage(familySamples: MetricFamilySamples): String = {
    constatntHelp
  }
}

class SparkDropwizardExports(private val registry: MetricRegistry,
                             override val metricsNameReplace: Option[NameDecorator.Replace],
                             override val extraLabels: Map[String, String],
                             override val maybeTimestamp: Option[Long])
  extends CollectorDecorator(new DropwizardExports(registry))
    with NameDecorator
    with LabelsDecorator
    with ConstantTimestampDecorator
    with ConstantHelpDecorator {
  override val constatntHelp: String = "Generated from Dropwizard metric import"
}

class SparkJmxExports(private val jmxCollector: JmxCollector,
                 override val extraLabels: Map[String, String],
                 override val maybeTimestamp: Option[Long])
  extends CollectorDecorator(jmxCollector)
    with LabelsDecorator
    with ConstantTimestampDecorator