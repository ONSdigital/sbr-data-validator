name := "sbr-data-validator"

version := "1.0"

scalaVersion := "2.11.8"

fork := true

parallelExecution in Test:= false

lazy val Versions = new {
  val hbase = "1.2.6"
  val spark = "2.2.0"
}

resolvers += "ClouderaRepo" at "https://repository.cloudera.com/artifactory/cloudera-repos"
resolvers += "Local Maven Repository" at "file:///Users/georgerushton/.m2/repository"
resolvers += Resolver.bintrayRepo("ons", "ONS-Registers")

libraryDependencies ++= Seq(
  "uk.gov.ons" % "registers-sml" % "1.12",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.apache.hbase" % "hbase-hadoop-compat" % "1.4.2",
  "com.typesafe" % "config" % "1.3.2",
  ("org.apache.hbase" % "hbase-server" % Versions.hbase)
    .exclude("com.sun.jersey","jersey-server")
    .exclude("org.mortbay.jetty","jsp-api-2.1"),
  "org.apache.hbase" % "hbase-common" % Versions.hbase,
  "org.apache.hbase" %  "hbase-client" % Versions.hbase,
  ("org.apache.spark" %% "spark-core" % Versions.spark)
    .exclude("aopalliance","aopalliance")
    .exclude("commons-beanutils","commons-beanutils"),
  "org.apache.spark" %% "spark-sql" % Versions.spark,
  ("org.apache.crunch" % "crunch-hbase" % "0.15.0")   .exclude("com.sun.jersey","jersey-server")
)


assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*)    => MergeStrategy.first
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.first
  case PathList("javax", "ws", xs @ _*) => MergeStrategy.first
  case PathList("jersey",".", xs @ _*) => MergeStrategy.first
  case PathList("aopalliance","aopalliance", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", xs @ _*)         => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
mainClass in (Compile,run) := Some("validator.Main")