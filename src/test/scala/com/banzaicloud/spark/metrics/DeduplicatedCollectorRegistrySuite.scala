package com.banzaicloud.spark.metrics

import com.codahale.metrics.MetricRegistry
import io.prometheus.client.{Collector, CollectorRegistry}
import io.prometheus.client.dropwizard.DropwizardExports
import org.junit.{Assert, Test}

import scala.collection.JavaConverters._

class DeduplicatedCollectorRegistrySuite {
    @Test def testDeduplication(): Unit = {
      // given
      val baseRegistry = new MetricRegistry
      val registryA = new MetricRegistry
      val counterA = registryA.counter("counter")
      counterA.inc(20)
      counterA.inc(30)

      val registryB = new MetricRegistry
      val counterB = registryB.counter("counter")
      counterB.inc(40)
      counterB.inc(50)
      baseRegistry.register("hive_", registryA)
      baseRegistry.register("hive.", registryB)

      val metricsExports = new DropwizardExports(baseRegistry)
      val deduplicatedCollectorRegistry = new DeduplicatedCollectorRegistry(new CollectorRegistry(true))

      // when
      metricsExports.register(deduplicatedCollectorRegistry)
      val samples = deduplicatedCollectorRegistry.metricFamilySamples()

      // then
      val actual = samples
        .asScala
        .filter(mfs => mfs.`type`== Collector.Type.GAUGE && mfs.name == "hive__counter")
      Assert.assertEquals(1, actual.size)
    }
}
