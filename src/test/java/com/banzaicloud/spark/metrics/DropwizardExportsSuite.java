package com.banzaicloud.spark.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.UniformReservoir;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Enumeration;

public class DropwizardExportsSuite {
    @Test
    public void testExportedMetricSamplesHelpString() {
        // given
        MetricRegistry registry = new MetricRegistry();
        CollectorRegistry pushRegistry = CollectorRegistry.defaultRegistry;
        com.banzaicloud.spark.metrics.DropwizardExports metricsExports = new com.banzaicloud.spark.metrics.DropwizardExports(registry);
        metricsExports.register(pushRegistry);


        Counter metric1 = new Counter();
        metric1.inc();
        Histogram metric2 = new Histogram(new UniformReservoir());
        metric2.update(2);

        registry.register("test-metric-sample1", metric1);
        registry.register("test-metric-sample2", metric2);

        // when
        Enumeration<Collector.MetricFamilySamples>  exportedMetrics = pushRegistry.metricFamilySamples();

        // then
        String expected = "Generated from Dropwizard metric import";
        for (Collector.MetricFamilySamples mfs: Collections.list(exportedMetrics)) {
            Assert.assertEquals(expected, mfs.help);
        }
    }
}
