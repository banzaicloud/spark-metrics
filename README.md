# Apache Spark metrics extensions

This is a repository for ApacheSpark metrics related custom classes (e.g. sources, sinks). We were trying to extend the Spark Metrics subsystem with a Prometheus sink but the [PR](https://github.com/apache/spark/pull/19775#issuecomment-371504349) was not merged upstream. In order to support others to use Prometheus we have externalized the sink and made available through this repository.

* [Promethes sink](https://github.com/banzaicloud/spark-metrics/blob/master/PrometheusSink.md)


