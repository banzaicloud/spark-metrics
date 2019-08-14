lazy val root = (project in file("."))
  .settings(
    name := "spark-metrics",
    organization := "com.banzaicloud",
    organizationHomepage := Some(url("https://banzaicloud.com")),
    homepage := Some(url("https://github.com/banzaicloud/park-metrics")),
    developers := List(
      Developer("stoader", "Sebastian Toader", "st0ad3r@gmail.com", url("https://github.com/stoader")),
      Developer("sancyx", "Sandor Magyari", "sancyx@gmail.com", url("https://github.com/sancyx")),
      Developer("baluchicken", "Balint Molnar", "balintmolnar91@gmail.com", url("https://github.com/baluchicken"))
    ),
    scmInfo := Some(ScmInfo(url("https://github.com/banzaicloud/park-metrics"), "git@github.com:banzaicloud/spark-metrics.git")),
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalaVersion := "2.11.12",
    version      := "2.3-2.1.0",
    libraryDependencies ++= Seq(
      "io.prometheus" % "simpleclient" % "0.3.0",
      "io.prometheus" % "simpleclient_dropwizard" % "0.3.0",
      "io.prometheus" % "simpleclient_pushgateway" % "0.3.0",
      "io.dropwizard.metrics" % "metrics-core" % "3.1.2",
      "org.slf4j" % "slf4j-api" % "1.7.16",
      "com.google.guava" % "guava" % "26.0-android",
      "io.prometheus.jmx" % "collector" % "0.10",
      "com.novocode" % "junit-interface" % "0.11" % Test
    )
  )


publishMavenStyle := true
useGpg := true

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

