package com.banzaicloud.spark.metrics;

import java.util.Properties

import org.junit.{Assert, Test}

class DropwizardExportsWithMetricNameTransformSuite {

    val props = new Properties()
    props.load(getClass.getResourceAsStream("/spark-prometheus-metrics.conf"))
    val regexExpr = props.getProperty("*.sink.prometheus.metrics-name-capture-regex")
    val regexReplacement = props.getProperty("*.sink.prometheus.metrics-name-replacement")

    val export = new DropwizardExportsWithMetricNameTransform(
        registry = null,
        regexExpr.r,
        regexReplacement,
        toLowerCase = true
    )
    val metric1 = "spark_0e665960e096446f97dbf56067cdc0b8_driver_DAGScheduler_stage_runningStages"
    val metric2 = "spark_0e665960e096446f97dbf56067cdc0b8_2_executor_filesystem_hdfs_read_bytes"
    val metric3 = "local_1575556728496_driver_LiveListenerBus_numEventsPosted"
    val metric4 = "metrics_spark_c1bd069243c54a249a02207158a2fbee_driver_livelistenerbus_listenerprocessingtime_org_apache_spark_status_appstatuslistener_count"

    @Test
    def testThatPrometheusSinkCanBeLoaded(): Unit = {
        val metricTransformed1 = export.transformMetricsName(metric1)
        val metricTransformed2 = export.transformMetricsName(metric2)
        val metricTransformed3 = export.transformMetricsName(metric3)
        val metricTransformed4 = export.transformMetricsName(metric4)

        Assert.assertEquals("driver_dagscheduler_stage_runningstages", metricTransformed1)
        Assert.assertEquals("executor_filesystem_hdfs_read_bytes", metricTransformed2)
        Assert.assertEquals("driver_livelistenerbus_numeventsposted", metricTransformed3)
        Assert.assertEquals("driver_livelistenerbus_listenerprocessingtime_org_apache_spark_status_appstatuslistener_count", metricTransformed4)
    }
}