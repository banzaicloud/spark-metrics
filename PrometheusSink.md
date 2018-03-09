# Prometheus sink

PrometheusSink is a Spark metrics [sink](https://spark.apache.org/docs/2.2.0/monitoring.html#metrics) that publishes spark metrics into [Prometheus](https://prometheus.io). 

## Prerequisites

Prometheus uses a **pull** model over http to scrape data from the applications. For batch jobs it also supports a **push** model. We need to use this model as Spark pushes metrics to sink. In order to enable this feature for Prometheus a special component called [pushgateway](https://github.com/prometheus/pushgateway) needs to be running.

## How to enable PrometheusSink in Spark

Spark publishes metrics to Sinks listed in the metrics configuration file. The localtion of the metrics configuration file can be specified for `spark-submit` as follows:

```sh
--conf spark.metrics.conf=<path_to_the_file>/metrics.properties
```

Add the following lines to metrics configuartion file:

```sh
# Enable Prometheus for all instances by class name
*.sink.prometheus.class=com.banzaicloud.spark.metrics.sink.PrometheusSink
# Prometheus pushgateway address
*.sink.prometheus.pushgateway-address-protocol=<prometheus pushgateway protocol> - defaults to http
*.sink.prometheus.pushgateway-address=<prometheus pushgateway address> - defaults to 127.0.0.1:9091
*.sink.prometheus.period=<period> - defaults to 10
*.sink.prometheus.unit=< unit> - defaults to seconds (TimeUnit.SECONDS)
*.sink.prometheus.pushgateway-enable-timestamp=<enable/disable metrics timestamp> - defaults to false
```

* **pushgateway-address-protocol** - the scheme of the URL where pushgateway service is available
* **pushgateway-address** - the host and port the URL where pushgateway service is available
* **period** - controls the periodicity of metrics being sent to pushgateway
* **unit** - the time unit of the periodicity
* **pushgateway-enable-timestamp** - controls whether to send the timestamp of the metrics sent to pushgateway. This is disabled by default as **not all** versions of pushgateway support timestamp for metrics.

`spark-submit` needs to know repository where to download the `jar` containing PrometheusSink from:

```sh
--repositories https://raw.github.com/banzaicloud/spark-metrics/master/maven-repo/releases
```

_**Note**_: this is a maven repo hosted on github

Also we have to specify the spark-metrics package that includes PromethusSink and it's dependendent packages for `spark-submit`:

```sh
--packages com.banzaicloud:spark-metrics_2.11:2.2.1-1.0.0,io.prometheus:simpleclient:0.0.23,io.prometheus:simpleclient_dropwizard:0.0.23,io.prometheus:simpleclient_pushgateway:0.0.23,io.dropwizard.metrics:metrics-core:3.1.2
```

## Package version

The version number of the package is formatted as: `com.banzaicloud:spark-metrics_<scala version>:<spark version>-<version>`