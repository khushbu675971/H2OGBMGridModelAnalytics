version := "0.1"
name := "H2O"

scalaVersion := "2.11.7"
val sparkVersion = "2.4.2"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "ai.h2o" %% "sparkling-water-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-repl" % sparkVersion
)
