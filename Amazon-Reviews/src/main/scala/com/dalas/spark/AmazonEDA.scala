package com.dalas.spark

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType, DateType}
import java.sql.Date
import java.nio.file.Paths
import System.nanoTime
import scala.util.Random
import scala.reflect.ClassTag

object AmazonEDA {

  case class Review(marketplace: String, customer_id: Int, review_id: String, product_id: String,
                    product_parent: Int, product_title: String, product_category: String,
                    star_rating: String, helpful_votes: Int, total_votes: Int, vine: String,
                    verified_purchase: String, review_headline: String, review_body: String, review_date: Date)

  def main(args: Array[String]) {

    // Start time
    val t1 = System.nanoTime()

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession.builder()
      .appName("Amazon Exploratory Data Analysis")
      .master("local[*]")
      .getOrCreate()

    val project_path = "src/main/resources/data/amazon/archive"
    val target_path = "src/main/resources/data/eda-results"

    // Set the log level to only print errors
    spark.sparkContext.setLogLevel("ERROR")

    // UDF to convert string star_rating to int safely
    val udfInt = udf((s: String) => if (s.forall(Character.isDigit)) s.toInt else -1)

    val reviewSchema = new StructType()
      .add("marketplace", StringType, nullable = true)
      .add("customer_id", IntegerType, nullable = true)
      .add("review_id", StringType, nullable = true)
      .add("product_id", StringType, nullable = true)
      .add("product_parent", IntegerType, nullable = true)
      .add("product_title", StringType, nullable = true)
      .add("product_category", StringType, nullable = true)
      .add("star_rating", StringType, nullable = true)
      .add("helpful_votes", IntegerType, nullable = true)
      .add("total_votes", IntegerType, nullable = true)
      .add("vine", StringType, nullable = true)
      .add("verified_purchase", StringType, nullable = true)
      .add("review_headline", StringType, nullable = true)
      .add("review_body", StringType, nullable = true)
      .add("review_date", DateType, nullable = true)

    import spark.implicits._
    val amazonDS = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(reviewSchema)
      .csv(project_path)
      .as[Review]

    // Safely casting star_rating from string to int
    val intRatingAmazonDS = amazonDS.withColumn("int_star_rating",
      when(col("star_rating").isNotNull, udfInt(col("star_rating"))).otherwise(lit(null)))


    val initialRowCount = intRatingAmazonDS.count()
    println(s"Count of initial DF: $initialRowCount")

    // Number of initial partitions: 87

    /* EDA Questions */

    // Q1: Counting nulls in relevant columns

    // Custom function that counts null values for each column_name in array
    def countCols(columns: Array[String]) = {
      columns.map(
        c => count(when(col(c).isNull, c)).alias(c)
      )
    }

    // Creating DataFrame with all same columns and their null counts (contains just one row)
    val nullCountDF = intRatingAmazonDS.select(countCols(intRatingAmazonDS.columns): _*)
    nullCountDF.write
      .mode(SaveMode.Overwrite)
      .save(Paths.get(target_path, "null-count").toString)


    // Cleaning: Dropping nulls in relevant columns
    val CleanAmazonDF = intRatingAmazonDS
      .na.drop(Seq(
      "customer_id",
      "review_id",
      "product_id",
      "product_parent",
      "product_title",
      "product_category",
      "star_rating",
      "review_body",
      "review_date"))
      .withColumn("word_count", size(split(col("review_body"), "\\s+")))
      .withColumn("year", year(col("review_date")))

    val cleanCount = CleanAmazonDF.count()
    println(s"Count post data cleansing: $cleanCount")
    println(s"Dropped off data: ${initialRowCount - cleanCount}")

    CleanAmazonDF.repartition(96)


    // Q2 Star Rating distribution

    val starRatingDist = CleanAmazonDF.select("product_id", "star_rating")

    starRatingDist.write
      .mode(SaveMode.Overwrite)
      .save(Paths.get(target_path, "star-rating-dist").toString)


    // Q3: Calculate and save average rating per product
    val avgProductRating = CleanAmazonDF
      .groupBy("product_category", "product_id")
      .agg(avg("star_rating").as("avg_stars"))
      .orderBy(col("avg_stars").desc_nulls_last)

    avgProductRating.write
      .mode(SaveMode.Overwrite)
      .save(Paths.get(target_path, "avg-product-rating").toString)


    // Q4: Generate basic statistics for relevant value columns
    val valueColDescription = CleanAmazonDF.select("star_rating", "helpful_votes", "total_votes").describe()
    valueColDescription.write
      .mode(SaveMode.Overwrite)
      .save(Paths.get(target_path, "describe").toString)


    // Q5: Generate word count distribution
    val wordCountDF = CleanAmazonDF.groupBy("word_count").agg(count("*").as("review_count"))
    wordCountDF.write
      .mode(SaveMode.Overwrite)
      .save(Paths.get(target_path, "word-count").toString)


    /* BUSINESS QUESTIONS */

    // Q6: How to detect products that should be dropped or kept available
    val prodIdList = CleanAmazonDF
      .select("product_id")
      .distinct()
      .map(f => f.getString(0))
      .collect().toList

    def takeSample[T: ClassTag](a: Array[T], n: Int, seed: Long) = {
      val rnd = new Random(seed)
      Array.fill(n)(a(rnd.nextInt(a.length)))
    }

    val prodIdSample = takeSample(prodIdList.toArray, 30, 42)

    val starRatingDistSampled = starRatingDist.filter(col("product_id").isin(prodIdSample: _*))
    starRatingDistSampled.write
      .mode(SaveMode.Overwrite)
      .save(Paths.get(target_path, "product-performance").toString)


    // Q7: Evolution by year and by product_category of the qty of reviews
    val reviewQtyEvolution = CleanAmazonDF
      .groupBy("year", "product_category")
      .agg(count("*").as("review_count"))

    reviewQtyEvolution.write
      .mode(SaveMode.Overwrite)
      .save(Paths.get(target_path, "review-qty-evolution").toString)


    // Q8: Is there a biased between verified-purchase reviews and those that are not?
    val verifiedBias = CleanAmazonDF
      .groupBy("verified_purchase", "product_category")
      .agg(avg("star_rating").as("avg_rating"))

    verifiedBias.write
      .mode(SaveMode.Overwrite)
      .save(Paths.get(target_path, "verified-bias").toString)

    spark.stop()


    // Printing runtime duration in minutes
    val duration = (System.nanoTime - t1) / 1e9d
    println(s"Run time: $duration")
  }

}
