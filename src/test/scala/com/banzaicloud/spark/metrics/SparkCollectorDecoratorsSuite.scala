package com.banzaicloud.spark.metrics

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
    lazy val exportedMetrics = pushRegistry.metricFamilySamples().asScala.toList
    lazy val counterFamily = exportedMetrics.find(_.`type`== Collector.Type.GAUGE).get
    lazy val histogramFamily = exportedMetrics.find(_.`type`== Collector.Type.SUMMARY).get
    lazy val counterSamples = counterFamily.samples.asScala
    lazy val histogramSamples = histogramFamily.samples.asScala

  }

  @Test def testStaticTimestamp(): Unit = new Fixture {
    // given
    val staticTs = PushTimestampDecorator.PushTimestampProvider(() => 1L)

    val metricsExports = new SparkDropwizardExports(registry, None, Map.empty, Some(staticTs))
    metricsExports.register(pushRegistry)

    // then
    exportedMetrics.foreach { family =>
      family.samples.asScala.foreach { sample =>
        Assert.assertEquals(1L, sample.timestampMs)
      }
    }
  }

  @Test def testNoPushTimestamp(): Unit = new Fixture {
    // given
    val metricsExports = new SparkDropwizardExports(registry, None, Map.empty, None)
    val jmxMetricsExports = new SparkJmxExports(jmxCollector, Map.empty, None)
    metricsExports.register(pushRegistry)
    jmxMetricsExports.register(pushRegistry)

    // then
    exportedMetrics.foreach { family =>
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
    Assert.assertTrue(exportedMetrics.size == 2)

    Assert.assertTrue(counterSamples.size == 1)
    counterSamples.foreach { sample =>
      Assert.assertTrue(s"[${sample.name}]", sample.name == "test_metric_sample__1__")
    }

    Assert.assertTrue(histogramSamples.size == 7)
    histogramSamples.foreach { sample =>
      Assert.assertTrue(sample.name.startsWith("test_metric_sample__2__"))
    }

  }

  @Test def testDeduplication(): Unit = new Fixture { // given
    val registry2 = new MetricRegistry
    val counterA = registry2.counter("counter")
    counterA.inc(20)
    counterA.inc(30)
    val registry3 = new MetricRegistry
    val counterB = registry3.counter("counter")
    counterB.inc(40)
    counterB.inc(50)
    registry.register("hive_", registry2)
    registry.register("hive.", registry3)


    val metricsExports = new SparkDropwizardExports(registry, None, Map.empty, None)
    metricsExports.register(pushRegistry)
    val counters = exportedMetrics.filter(mfs => mfs.`type`== Collector.Type.GAUGE && mfs.name == "hive__counter")
    Assert.assertEquals(1, counters.size)
  }

  @Test def testStaticHelpMessage(): Unit = new Fixture { // given
    val staticTs = 1

    val metricsExports = new SparkDropwizardExports(registry, None, Map.empty, None)
    metricsExports.register(pushRegistry)

    // then
    val expectedHelp = "Generated from Dropwizard metric import"
    exportedMetrics.foreach { family =>
      Assert.assertEquals(expectedHelp, family.help)
    }
  }

  @Test def testExtraLabels(): Unit = new Fixture { // given
    val metricsExports = new SparkDropwizardExports(registry, None, extraLabels, None)
    val jmxMetricsExports = new SparkJmxExports(jmxCollector, extraLabels, None)
    metricsExports.register(pushRegistry)
    jmxMetricsExports.register(pushRegistry)

    // then
    Assert.assertTrue(exportedMetrics.size > 2)

    exportedMetrics.foreach { family =>
      family.samples.asScala.foreach { sample =>
        Assert.assertTrue(sample.labelNames.asScala.toList.containsSlice(extraLabels.keys.toList))
        Assert.assertTrue(sample.labelValues.asScala.toList.containsSlice(extraLabels.values.toList))
      }
    }
  }
}
