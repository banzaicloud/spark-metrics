package com.banzaicloud.spark.metrics

import java.util.concurrent.atomic.AtomicLong

import com.codahale.metrics.MetricRegistry
import io.prometheus.client.{Collector, CollectorRegistry}
import io.prometheus.jmx.JmxCollector
import org.junit.{Assert, Test}

import scala.collection.JavaConverters._


class SparkCollectorDecoratorsSuite {
  trait Fixture {
    // Given
    val extraLabels = Map("label1" -> "val1", "label2" -> "val2")
    val registry = new MetricRegistry
    val jmxCollector = new JmxCollector("")
    val pushRegistry = new CollectorRegistry(true)
    val metric1 = registry.counter("test-metric-sample1")
    metric1.inc()
    val metric2 = registry.histogram("test-metric-sample2")
    metric2.update(2)

    // Then
    def getExportedMetrics = pushRegistry.metricFamilySamples().asScala.toList
    def getCounterFamily = getExportedMetrics.find(_.`type`== Collector.Type.GAUGE).get
    def getHistogramFamily = getExportedMetrics.find(_.`type`== Collector.Type.SUMMARY).get
    def getCounterSamples = getCounterFamily.samples.asScala
    def getHistogramSamples = getHistogramFamily.samples.asScala

  }

  @Test def testStaticTimestamp(): Unit = new Fixture {
    // given
    val ts = new AtomicLong(0)
    val pushTs = PushTimestampDecorator.PushTimestampProvider(() => ts.incrementAndGet())

    val metricsExports = new SparkDropwizardExports(registry, None, Map.empty, Some(pushTs))
    metricsExports.register(pushRegistry)

    def testTimestampEqual() = {
      val allTimestamps = getExportedMetrics.flatMap { family =>
        family.samples.asScala.map(_.timestampMs)
      }
      Assert.assertSame("Samples were omitted.", 8, allTimestamps.size)
      Assert.assertSame("Timestamps are not the same.",1, allTimestamps.distinct.size)
  }

    // then
    testTimestampEqual()
    testTimestampEqual()
    testTimestampEqual()
  }

  @Test def testNoPushTimestamp(): Unit = new Fixture {
    // given
    val metricsExports = new SparkDropwizardExports(registry, None, Map.empty, None)
    val jmxMetricsExports = new SparkJmxExports(jmxCollector, Map.empty, None)
    metricsExports.register(pushRegistry)
    jmxMetricsExports.register(pushRegistry)

    // then
    getExportedMetrics.foreach { family =>
      family.samples.asScala.foreach { sample =>
        Assert.assertEquals(null, sample.timestampMs)
      }
    }
  }

  @Test def testNameReplace(): Unit = new Fixture { // given
    val metricsNameReplace = NameDecorator.Replace("(\\d+)".r, "__$1__")

    val metricsExports = new SparkDropwizardExports(registry, Some(metricsNameReplace), Map.empty, None)
    metricsExports.register(pushRegistry)

    // then
    Assert.assertTrue(getExportedMetrics.size == 2)

    val counterSamples = getCounterSamples
    Assert.assertTrue(counterSamples.size == 1)
    counterSamples.foreach { sample =>
      Assert.assertTrue(s"[${sample.name}]", sample.name == "test_metric_sample__1__")
    }

    val histogramSamples = getHistogramSamples
    Assert.assertTrue(histogramSamples.size == 7)
    histogramSamples.foreach { sample =>
      Assert.assertTrue(sample.name.startsWith("test_metric_sample__2__"))
    }

  }

  @Test def testStaticHelpMessage(): Unit = new Fixture { // given
    val staticTs = 1

    val metricsExports = new SparkDropwizardExports(registry, None, Map.empty, None)
    metricsExports.register(pushRegistry)

    // then
    val expectedHelp = "Generated from Dropwizard metric import"
    getExportedMetrics.foreach { family =>
      Assert.assertEquals(expectedHelp, family.help)
    }
  }

  @Test def testExtraLabels(): Unit = new Fixture { // given
    val metricsExports = new SparkDropwizardExports(registry, None, extraLabels, None)
    val jmxMetricsExports = new SparkJmxExports(jmxCollector, extraLabels, None)
    metricsExports.register(pushRegistry)
    jmxMetricsExports.register(pushRegistry)

    // then
    val exportedMetrics = getExportedMetrics
    Assert.assertTrue(exportedMetrics.size > 2)

    exportedMetrics.foreach { family =>
      family.samples.asScala.foreach { sample =>
        Assert.assertTrue(sample.labelNames.asScala.toList.containsSlice(extraLabels.keys.toList))
        Assert.assertTrue(sample.labelValues.asScala.toList.containsSlice(extraLabels.values.toList))
      }
    }
  }
}
