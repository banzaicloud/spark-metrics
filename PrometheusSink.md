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
*.sink.prometheus.class=org.apache.spark.banzaicloud.metrics.sink.PrometheusSink
# Prometheus pushgateway address
*.sink.prometheus.pushgateway-address-protocol=<prometheus pushgateway protocol> - defaults to http
*.sink.prometheus.pushgateway-address=<prometheus pushgateway address> - defaults to 127.0.0.1:9091
*.sink.prometheus.period=<period> - defaults to 10
*.sink.prometheus.unit=< unit> - defaults to seconds (TimeUnit.SECONDS)
*.sink.prometheus.pushgateway-enable-timestamp=<enable/disable metrics timestamp> - defaults to false
# Metrics name processing (version 2.3-1.1.0 +)
*.sink.prometheus.metrics-name-capture-regex=<regular expression to capture sections metric name sections to be replaces>
*.sink.prometheus.metrics-name-replacement=<replacement captured sections to be replaced with>
*.sink.prometheus.labels=<labels in label=value format separated by comma>
# Support for JMX Collector (version 2.3-2.0.0 +)
*.sink.prometheus.enable-dropwizard-collector=false
*.sink.prometheus.enable-jmx-collector=true
*.sink.prometheus.jmx-collector-config=/opt/spark/conf/monitoring/jmxCollector.yaml

# Enable HostName in Instance instead of Appid (Default value is false i.e. instance=${appid})
*.sink.prometheus.enable-hostname-in-instance=true

# Enable JVM metrics source for all instances by class name
*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink
*.source.jvm.class=org.apache.spark.metrics.source.JvmSource

# Use custom metric filter
*.sink.prometheus.metrics-filter-class=com.example.RegexMetricFilter
*.sink.prometheus.metrics-filter-regex=com.example\.(.*)
```

* **pushgateway-address-protocol** - the scheme of the URL where pushgateway service is available
* **pushgateway-address** - the host and port the URL where pushgateway service is available
* **period** - controls the periodicity of metrics being sent to pushgateway
* **unit** - the time unit of the periodicity
* **pushgateway-enable-timestamp** - controls whether to send the timestamp of the metrics sent to pushgateway. This is disabled by default as **not all** versions of pushgateway support timestamp for metrics.
* **metrics-name-capture-regex** - if provided than this regexp is applied on each metric name prior sending to Prometheus. The metric name sections captured(regexp groups) will be replaced with the value passed in `metrics-name-replacement`.
e.g. `(.*driver_)(.+)`. *Supported only in version **2.3-1.1.0 and above**.*
* **metrics-name-replacement** - the replacement to replace captured sections(regexp groups) metric name. e.g. `${2}`. *Supported only in version **2.3-1.1.0 and above**.*
* **labels** - the list of labels to be passed to Prometheus with each metrics in addition to the default ones. This must be specified in the format label=value sperated by comma. *Supported only in version **2.3-1.1.0 and above**.*
* **group-key** - the list of labels to be passed to pushgateway as a group key. This must be specified in the label=value format separated by comma. `role`, `number` labels are always added even if custom group key is set. If `group-key` is not specified PrometheusSink will use `role`, `app_name`, `instance`, `number` and all metrics labels as a group key.
Please note that `instance` name change between application executions, so pushgateway will create and keep metrics groups from the old runs. This may lead to memory issues after many runs. That's way setting `group-key` is strongly advised.  *Supported only in version **2.4-1.0.0 and above**.*
* **enable-dropwizard-collector** - from version 2.3-2.0.0 you can enable/disable dropwizard collector
* **enable-jmx-collector** - from version 2.3-2.0.0 you can enable/disable JMX collector which collects configure metrics from JMX
* **jmx-collector-config** - the location of jmx collector config file
* **metrics-filter-class** - optional MetricFilter implementation to filter metrics to report. By default, all metrics are reported.
* **metrics-filter-&lt;key&gt;** - configuration option passed to the metric filter's constructor. See more on this below.

#### JMX configuration

Example JMX collector configuration file:

```sh
    lowercaseOutputName: false
    lowercaseOutputLabelNames: false
    whitelistObjectNames: ["*:*"]
```

You can find more detailed description on this configuration file [here](https://github.com/prometheus/jmx_exporter).

#### Metrics filtering

It is possible to provide a custom class implementing [`com.codahale.metrics.MetricFilter`](https://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/MetricFilter.html)
for filtering metrics to report. The qualified class name should be specified with the **metrics-filter-class** configuration option.
The class must have a public constructor taking either
 - `scala.collection.immutable.Map[String, String]`
 - `java.util.Map[String, String]`
 - `java.util.Properties`.
 
Properties matching **metrics-filter-&lt;key&gt;** are passed as key-value pairs to an applicable constructor during
instantiation. Additionally, if we can't find any of the above, the public no-arg constructor is tried, but in this case
no configuration can be passed.
  
#### Deploying

`spark-submit` needs to know repository where to download the `jar` containing PrometheusSink from:

```sh
--repositories https://raw.github.com/banzaicloud/spark-metrics/master/maven-repo/releases
```

_**Note**_: this is a maven repo hosted on GitHub. Starting from version 2.3-2.1.0 PrometheusSink is available through Maven Central Repo under group id `com.banzaicloud` thus passing `--repositories` to `spark-submit`
is not needed. 

Also we have to specify the spark-metrics package that includes PrometheusSink and it's dependent packages for `spark-submit`:
*  Spark 2.2:

```sh
--packages com.banzaicloud:spark-metrics_2.11:2.2.1-1.0.0,io.prometheus:simpleclient:0.0.23,io.prometheus:simpleclient_dropwizard:0.0.23,io.prometheus:simpleclient_pushgateway:0.0.23,io.dropwizard.metrics:metrics-core:3.1.2
```

* Spark 2.3:

```sh
--packages com.banzaicloud:spark-metrics_2.11:2.3-2.1.0,io.prometheus:simpleclient:0.3.0,io.prometheus:simpleclient_dropwizard:0.3.0,io.prometheus:simpleclient_pushgateway:0.3.0,io.dropwizard.metrics:metrics-core:3.1.2
```

* Spark 3.1:

```sh
--packages com.banzaicloud:spark-metrics_2.12:3.1-1.0.0,io.prometheus:simpleclient:0.11.0,io.prometheus:simpleclient_dropwizard:0.11.0,io.prometheus:simpleclient_pushgateway:0.11.0,io.prometheus:simpleclient_common:0.11.0,io.prometheus.jmx:collector:0.15.0
```
_**Note**_: the `--packages` option currently is not supported by _**Spark 2.3 when running on Kubernetes**_. The reason is that the `--packages` option behind the scenes downloads the files from maven repo to local machines than uploads these to the cluster using _Local File Dependency Management_ feature. This feature has not been backported from the _**Spark 2.2 on Kubernetes**_ fork to _**Spark 2.3**_. See details here: [Spark 2.3 Future work](https://spark.apache.org/docs/latest/running-on-kubernetes.html#future-work). This can be worked around by:
1. building ourselves [Spark 2.3 with Kubernetes support](https://spark.apache.org/docs/latest/building-spark.html#building-with-kubernetes-support) from source
1. downloading and adding the dependencies to `assembly/target/scala-2.11/jars` folder, by running the following commands:

   1. export SPARK_METRICS_VERSION=2.3-2.1.0
   1. mvn dependency:get -DgroupId=com.banzaicloud -DartifactId=spark-metrics_2.11 -Dversion=${SPARK_METRICS_VERSION}
   1. mkdir temp
   1. mvn dependency:copy-dependencies -f ~/.m2/repository/com/banzaicloud/spark-metrics_2.11/${SPARK_METRICS_VERSION}/spark-metrics_2.11-${SPARK_METRICS_VERSION}.pom -DoutputDirectory=$(pwd)/temp
   1. cp temp/* assembly/target/scala-2.11/jars
   
### Build fat jar
In the case when you want to add spark-metrics jar into your classpath you can build it locally to include all needed dependencies using the following command: 
```sh
sbt assembly
```
After assembling just add resulting ``spark-metrics-assembly-3.1-1.0.0.jar`` into your Spark classpath (e.g. `$SPARK_HOME/jars/`).

#### Package version

The version number of the package is formatted as: `com.banzaicloud:spark-metrics_<scala version>:<spark version>-<version>`

### Conclusion

This is not the ideal scenario but it perfectly does the job and it's independent of Spark core. At [Banzai Cloud](https://banzaicloud.com) we are **willing to contribute** this sink once the community decided that it actually needs it. Meanwhile you can open a new feature requests, use this existing PR or use any other means to ask for `native` Prometheus support and let us know through one of our social channels. As usual, we are happy to help. All we create is open source and at the same time all we consume is open source as well - so we are always eager to make open source projects better.
