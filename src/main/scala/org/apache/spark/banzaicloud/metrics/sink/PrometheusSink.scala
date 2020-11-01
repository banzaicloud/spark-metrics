package org.apache.spark.banzaicloud.metrics.sink

import java.net.URL
import java.util.Properties

import com.banzaicloud.spark.metrics.sink.PrometheusSink.SinkConfig
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import io.prometheus.client.exporter.PushGateway
import org.apache.spark.banzaicloud.metrics.sink.PrometheusSink.SinkConfigProxy
import org.apache.spark.internal.config
import org.apache.spark.metrics.sink.Sink
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}

object PrometheusSink {

  class SinkConfigProxy extends SinkConfig {
    // SparkEnv may become available only after metrics sink creation thus retrieving
    // SparkConf from spark env here and not during the creation/initialisation of PrometheusSink.
    @transient
    private lazy val sparkConfig = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf(true))

    // Don't use sparkConf.getOption("spark.metrics.namespace") as the underlying string won't be substituted.
    def metricsNamespace: Option[String] = sparkConfig.get(config.METRICS_NAMESPACE)
    def sparkAppId: Option[String] = sparkConfig.getOption("spark.app.id")
    def sparkAppName: Option[String] = sparkConfig.getOption("spark.app.name")
    def executorId: Option[String] = sparkConfig.getOption("spark.executor.id")
  }
}

class PrometheusSink(property: Properties,
                     registry: MetricRegistry,
                     securityMgr: SecurityManager,
                     sinkConfig: SinkConfig,
                     pushGatewayBuilder: URL => PushGateway
                    )
  extends com.banzaicloud.spark.metrics.sink.PrometheusSink(property, registry, sinkConfig, pushGatewayBuilder) with Sink {

  // Constructor required by MetricsSystem::registerSinks()
  def this(property: Properties, registry: MetricRegistry, securityMgr: SecurityManager) = {
    this(
      property,
      registry,
      securityMgr,
      new SinkConfigProxy,
      new PushGateway(_)
    )
  }
}