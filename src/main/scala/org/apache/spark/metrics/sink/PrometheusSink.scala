package org.apache.spark.metrics.sink

import java.util.Properties

import com.codahale.metrics.MetricRegistry
import org.apache.spark.SecurityManager

class PrometheusSink(val property: Properties,
                     val registry: MetricRegistry,
                     val securityMgr: SecurityManager)
  extends com.banzaicloud.spark.metrics.sink.PrometheusSink(property, registry) with Sink