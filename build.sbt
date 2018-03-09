lazy val root = (project in file("."))
  .settings(
    name := "spark-metrics",
    organization := "com.banzaicloud",
    scalaVersion := "2.11.12",
    version      := "2.2.1-1.0.0",
    libraryDependencies ++= Seq(
      "io.prometheus" % "simpleclient" % "0.0.23",
      "io.prometheus" % "simpleclient_dropwizard" % "0.0.23",
      "io.prometheus" % "simpleclient_pushgateway" % "0.0.23",
      "io.dropwizard.metrics" % "metrics-core" % "3.1.2",
      "org.slf4j" % "slf4j-api" % "1.7.25",
      "com.google.guava" % "guava" % "24.0-jre",
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