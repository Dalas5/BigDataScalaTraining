name := "Amazon-Reviews"

version := "1.0"

scalaVersion := "2.12.15"

val sparkVersion = "3.3.0"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scala-lang" % "scala-reflect" % "2.13.10",

  // ML
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "4.2.1",

  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.19.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.19.0"
)
