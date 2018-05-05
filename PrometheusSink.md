### Monitor Apache Spark with Prometheus the `alternative` way

We have externalized the sink into a separate library thus you can use this by either building for yourself or take it from our Maven repository.

#### Prometheus sink

PrometheusSink is a Spark metrics [sink](https://spark.apache.org/docs/2.2.0/monitoring.html#metrics) that publishes spark metrics into [Prometheus](https://prometheus.io). 

#### Prerequisites

Prometheus uses a **pull** model over http to scrape data from the applications. For batch jobs it also supports a **push** model. We need to use this model as Spark pushes metrics to sinks. In order to enable this feature for Prometheus a special component called [pushgateway](https://github.com/prometheus/pushgateway) needs to be running.

#### How to enable PrometheusSink in Spark

Spark publishes metrics to Sinks listed in the metrics configuration file. The location of the metrics configuration file can be specified for `spark-submit` as follows:

```sh
--conf spark.metrics.conf=<path_to_the_metrics_properties_file>
```

Add the following lines to metrics configuration file:

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

_**Note**_: this is a maven repo hosted on GitHub

Also we have to specify the spark-metrics package that includes PrometheusSink and it's dependent packages for `spark-submit`:
*  Spark 2.2:

```sh
--packages com.banzaicloud:spark-metrics_2.11:2.2.1-1.0.0,io.prometheus:simpleclient:0.0.23,io.prometheus:simpleclient_dropwizard:0.0.23,io.prometheus:simpleclient_pushgateway:0.0.23,io.dropwizard.metrics:metrics-core:3.1.2
```

* Spark 2.3:

```sh
--packages com.banzaicloud:spark-metrics_2.11:2.3-1.0.0,io.prometheus:simpleclient:0.3.0,io.prometheus:simpleclient_dropwizard:0.3.0,io.prometheus:simpleclient_pushgateway:0.3.0,io.dropwizard.metrics:metrics-core:3.1.2
```
_**Note**_: the `--packages` option currently is not supported by _**Spark 2.3 when running on Kubernetes**_. The reason is that the `--packages` option behind the scenes downloads the files from maven repo to local machines than uploads these to the cluster using _Local File Dependency Management_ feature. This feature has not been backported from the _**Spark 2.2 on Kubernetes**_ fork to _**Spark 2.3**_. See details here: [Spark 2.3 Future work](https://spark.apache.org/docs/latest/running-on-kubernetes.html#future-work). This can be worked around by:
1. building ourselves [Spark 2.3 with Kubernetes support](https://spark.apache.org/docs/latest/building-spark.html#building-with-kubernetes-support) from source
1. downloading and adding the following jars to `assembly/target/scala-2.11/jars`:
   1. com.banzaicloud:spark-metrics_2.11:2.3-1.0.0
   1. io.prometheus:simpleclient:0.3.0
   1. io.prometheus:simpleclient_common:0.3.0
   1. io.prometheus:simpleclient_dropwizard:0.3.0
   1. io.prometheus:simpleclient_pushgateway:0.3.0
   1. io.dropwizard.metrics:metrics-core:3.1.2
1. building [Spark docker images for Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html#docker-images)  

#### Package version

The version number of the package is formatted as: `com.banzaicloud:spark-metrics_<scala version>:<spark version>-<version>`

### Conclusion 

This is not the ideal scenario but it perfectly does the job and it's independent of Spark core. At [Banzai Cloud](https://banzaicloud.com) we are **willing to contribute** this sink once the community decided that it actually needs it. Meanwhile you can open a new feature requests, use this existing PR or use any other means to ask for `native` Prometheus support and let us know through one of our social channels. As usual, we are happy to help. All we create is open source and at the same time all we consume is open source as well - so we are always eager to make open source projects better. 
