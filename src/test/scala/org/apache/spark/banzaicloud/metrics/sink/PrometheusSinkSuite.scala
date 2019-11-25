package org.apache.spark.banzaicloud.metrics.sink

import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.{SecurityManager, SparkConf}
import org.junit.{After, Before, Test}

class PrometheusSinkSuite {
  private val sinkClassPropertyName = "spark.metrics.conf.*.sink.prometheus.class"
  private val sinkClassPropertyValue = "org.apache.spark.banzaicloud.metrics.sink.PrometheusSink"

  @Test
  def testThatPrometheusSinkCanBeLoaded() = {
    val instance = "driver"
    val conf = new SparkConf(true)
    val sm = new SecurityManager(conf)
    val ms = MetricsSystem.createMetricsSystem(instance, conf, sm)
    ms.start()
    ms.stop()
  }

  @Before
  def tearDown(): Unit = {
    System.setProperty(sinkClassPropertyName, sinkClassPropertyValue)
  }

  @After
  def setUp(): Unit = {
    System.clearProperty(sinkClassPropertyName)
  }
}
