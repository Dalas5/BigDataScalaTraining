package com.dalas.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.linalg.{DenseVector, VectorUDT}

object AmazonModelSimpleTest extends App{

  // Read data
  // Create a SparkSession using every core of the local machine
  val spark = SparkSession
    .builder
    .appName("Amazon NLP Model Simple Test")
    .master("local[*]")
    .getOrCreate()

  // Set the log level to only print errors
  spark.sparkContext.setLogLevel("ERROR")

  def avg_wordvecs_fun = (wordvecs: Seq[Seq[Double]]) => {
    val length = wordvecs.length
    val maxLength = wordvecs.maxBy(_.length).length
    val zeroSeq = Seq.fill[Double](maxLength)(0.0)
    val sumSeq = wordvecs.foldLeft(zeroSeq)((a, x) => (a zip x).map { case (u, v) => u + v })
    val averageSeq = sumSeq.map(_ / length.toDouble)
    new DenseVector(averageSeq.toArray)
  }

  val avg_wordvecs = spark.udf.register("avg_wordvecs", avg_wordvecs_fun)


  val nlp_pipeline = PipelineModel.load("src/main/resources/data/amazon/ml-results/nlp_pipeline.3.12")
  val model = PipelineModel.load("src/main/resources/data/amazon/ml-results/model.3.12")

  import spark.implicits._

  val data = Seq("I'm totally disappointed, It broke the next day I used",
    "I loved this product, it's great quality",
    "It is ok for the price, but I don't recommend it",
    "I hoped for more, but is acceptable quality, three stars")
    .toDF("review_body")

  val nlp_procd = nlp_pipeline.transform(data)

  val dataTransformed = nlp_procd.selectExpr(
    "review_body",
    "normalized.result AS normalized",
    "embeddings.embeddings")

  val demoWithAvg = dataTransformed
    .withColumn("avg_wordvec", avg_wordvecs(col("embeddings")))
    .drop("embeddings")


  val preds = model.transform(demoWithAvg)

  preds.select("review_body", "prediction").show(false)

}
