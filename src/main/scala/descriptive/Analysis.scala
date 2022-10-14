package descriptive

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataType, FloatType, IntegerType, StringType, StructType}

object Analysis {

  case class Review(marketplace: String, customer_id: String, review_id: String, product_id: String, product_parent: String,
                    product_title: String, product_category: String, star_rating: String, helpful_votes: Int, total_votes: Int,
                    vine: String, verified_purchase: String, review_headline: String, review_body: String, review_date: String)

  def main(args: Array[String]): Unit = {
    /**
     * Amazon dataset
     * Basic Analysis
     *
     */

    if (args.length != 2) {
      println("Need input and output path")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Amazon Dataset Exploration")
      .getOrCreate()

    val udfInt = udf((s: String) => if (s.forall(Character.isDigit)) s.toInt else -1)

    val reviewSchema = new StructType()
      .add("marketplace", StringType, nullable = true)
      .add("customer_id", StringType, nullable = true)
      .add("review_id", StringType, nullable = true)
      .add("product_id", StringType, nullable = true)
      .add("product_parent", StringType, nullable = true)
      .add("product_title", StringType, nullable = true)
      .add("product_category", StringType, nullable = true)
      .add("star_rating", StringType, nullable = true)
      .add("helpful_votes", IntegerType, nullable = true)
      .add("total_votes", IntegerType, nullable = true)
      .add("vine", StringType, nullable = true)
      .add("verified_purchase", FloatType, nullable = true)
      .add("review_headline", StringType, nullable = true)
      .add("review_body", IntegerType, nullable = true)
      .add("review_date", StringType, nullable = true)

    import spark.implicits._
    val amazonDS = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(reviewSchema)
      .csv(args(0))
      .as[Review]

    val intRatingAmazonDS = amazonDS.withColumn("int_star_rating",
      when(col("star_rating").isNotNull, udfInt(col("star_rating"))).otherwise(lit(null)))


    // Display actual schema of DS
    intRatingAmazonDS.printSchema()

    // Display numPartitions
    val numPartitions = intRatingAmazonDS.rdd.getNumPartitions
    println(s"Number of partitions: $numPartitions")

    // Display unique values of Star Rating after applying udf
    intRatingAmazonDS.select($"int_star_rating").distinct().show()

    // Print average Star Rating by Product Category
    intRatingAmazonDS.filter($"int_star_rating" >= 1).groupBy("product_category")
      .agg(round(avg("int_star_rating"), 2)).show()

    // Show the review with the highest Helpful Votes in each category
    intRatingAmazonDS.groupBy("product_category")
      .max("helpful_votes").show()


    // Calculate and save average rating per product
    val avgProductRating = intRatingAmazonDS
      .groupBy("product_id", "product_title", "product_category")
      .agg(avg("star_rating").as("avg_stars"))
      .orderBy(col("avg_stars").desc_nulls_last)

    avgProductRating.write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save(args(1))


  }

}
