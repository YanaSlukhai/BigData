name         := "BigDataCourses"
version      := "1.0"
organization := "epam.com"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"
val spec2Version = "4.3.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.specs2" %% "specs2-core" % spec2Version % Test,
  "org.specs2" %% "specs2-junit" % spec2Version % Test
)