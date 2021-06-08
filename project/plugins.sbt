resolvers += "Maven2 repository" at "https://repo1.maven.org/maven2/"

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "2.0.0-M2")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.0.0")
