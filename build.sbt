lazy val root = (project in file("."))
  .settings(
    name := "spark-metrics",
    organization := "com.banzaicloud",
    scalaVersion := "2.11.12",
    version      := "2.3-1.1.0",
    libraryDependencies ++= Seq(
      "io.prometheus" % "simpleclient" % "0.3.0",
      "io.prometheus" % "simpleclient_dropwizard" % "0.3.0",
      "io.prometheus" % "simpleclient_pushgateway" % "0.3.0",
      "io.dropwizard.metrics" % "metrics-core" % "3.1.2",
      "org.slf4j" % "slf4j-api" % "1.7.16",
      "com.google.guava" % "guava" % "14.0.1",
      "io.prometheus.jmx" % "collector" % "0.10",
      "com.novocode" % "junit-interface" % "0.11" % Test
    )
  )

publishMavenStyle := true

publishTo := {
  if (isSnapshot.value)
    Some(Resolver.file("file",  new File( "maven-repo/snapshots" )) )
  else
    Some(Resolver.file("file",  new File( "maven-repo/releases" )) )
}