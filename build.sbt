name := "task"

version := "0.1"

scalaVersion := "2.12.12"


val sparkVersion = "3.0.0"
val scalaTestVersion = "3.2.2"
val scalaCheckVersion = "1.14.3"
val sparkTestingBaseVersion = "2.4.5_0.14.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-repl" % sparkVersion,

  // tests
  "com.holdenkarau" %% "spark-testing-base" % sparkTestingBaseVersion % "test")
