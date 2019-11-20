package org.apache.spark.banzaicloud.metrics.sink

import java.util.Properties

import com.codahale.metrics.MetricRegistry
import org.apache.spark.SecurityManager
import org.apache.spark.metrics.sink.Sink

class PrometheusSink(val property: Properties,
                     val registry: MetricRegistry,
                     val securityMgr: SecurityManager)
  extends com.banzaicloud.spark.metrics.sink.PrometheusSink(property, registry) with Sink