package com.banzaicloud.spark.metrics

import java.util.Properties
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

  @Test
  def testNameReplaceFromProperties(): Unit = new Fixture {

    val props = new Properties()
    props.load(getClass.getResourceAsStream("/spark-prometheus-metrics.conf"))
    val regexExpr = props.getProperty("*.sink.prometheus.metrics-name-capture-regex")
    val regexReplacement = props.getProperty("*.sink.prometheus.metrics-name-replacement")

    val metricsNameReplace = NameDecorator.Replace(
      regexExpr.r,
      regexReplacement,
      toLowerCase = true
    )

    val metricsExports = new SparkDropwizardExports(registry, Some(metricsNameReplace), Map.empty, None)
    registry.counter("spark_0e665960e096446f97dbf56067cdc0b8_driver_DAGScheduler_stage_runningStages").inc()
    registry.counter("spark_0e665960e096446f97dbf56067cdc0b8_2_executor_filesystem_hdfs_read_bytes").inc()
    registry.counter("local_1575556728496_driver_LiveListenerBus_numEventsPosted").inc()
    registry.counter("metrics_spark_c1bd069243c54a249a02207158a2fbee_driver_livelistenerbus_listenerprocessingtime_org_apache_spark_status_appstatuslistener_count").inc()
    metricsExports.register(pushRegistry)

    val exportedMetricNames = getExportedMetrics.map(_.name)
    exportedMetricNames.contains("driver_dagscheduler_stage_runningstages")
    exportedMetricNames.contains("executor_filesystem_hdfs_read_bytes")
    exportedMetricNames.contains("driver_livelistenerbus_numeventsposted")
    exportedMetricNames.contains("driver_livelistenerbus_listenerprocessingtime_org_apache_spark_status_appstatuslistener_count")
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
