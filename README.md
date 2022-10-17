
# BigDataScalaTraining
# Amazon US Customer Reviews Dataset

<hr style=\"border:0.5px solid gray\"> </hr>

## Data
Amazon Customer Reviews (a.k.a. Product Reviews) is one of Amazon’s iconic products. In a period of over two decades since the first review in 1995, millions of Amazon customers have contributed over a hundred million reviews to express opinions and describe their experiences regarding products on the Amazon.com website. The dataset used in this project is a subset of 11.6 GB from the original 54.41 GB worth of customer reviews.<br>

Dataset rows				:  23,483,067 <br>
Dataset columns: 		: 15 <br>
Dataset size				: 11.6 GB <br>
Original Dataset link				: https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset
<hr style=\"border:0.5px solid gray\"> </hr>

## Data Description


| S.No |	Name |	Description |
|-------|------|--------------|
|1  |marketplace	|2 letter country code of the marketplace where the review was written.
|2	|customer_id	|Random identifier that can be used to aggregate reviews written by a single author.
|3	|review_id	|The unique ID of the review.
|4	|product_id	|The unique Product ID the review pertains to. In the multilingual dataset the reviews for the same product in different countries can be grouped by the same product_id. 
|5	|product_parent	|Random identifier that can be used to aggregate reviews for the same product.
|6	|product_title	|Title of the product.
|7	|product_category	|Broad product category that can be used to group reviews. (also used to group the dataset into coherent parts). 
|8	|star_rating	|The 1-5 star rating of the review.
|9	|helpful_votes	|Number of helpful votes.
|10	|total_votes	|Number of total votes the review received.
|11	|vine	|Review was written as part of the Vine program.
|12	|verified_purchase	|The review is on a verified purchase.
|13	|review_headline	|The title of the review.
|14	|review_body	|The review text.
|15	|review_date	|The date the review was written.

<hr style=\"border:0.5px solid gray\"> </hr>

## Exploratory Data Analysis Performed using Spark
### Reading the data
The SparkSession configuration was set to run on local for this project, making sure to use all of the available cpu cores.

```scala
    val spark = SparkSession.builder()
      .appName("Amazon Dataset Exploration")
      .master("local[*]")
      .getOrCreate()
```

A specific schema was defined in order to avoid Spark incurring in processing time by infering the schema of the data. Also a case class was created to parse the DataFrame as DataSet.
```scala
  case class Review(marketplace: String, customer_id: Int, review_id: String, product_id: String, product_parent: Int,
                    product_title: String, product_category: String, star_rating: String, helpful_votes: Int, total_votes: Int,
                    vine: String, verified_purchase: String, review_headline: String, review_body: String, review_date: Date)
```

```scala
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
```

```scala
    import spark.implicits._
    val amazonDS = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .schema(reviewSchema)
      .csv(project_path)
      .as[Review]
```

## Data cleansing

As a first sample, the "star_rating" column unique values were displayed, and many inconsistencies where found.
```scala
+--------------------+
|         star_rating|
+--------------------+
|                   3|
|                null|
|          2015-06-05|
|                   5|
|                   1|
|                   4|
|                   2|
|          2015-02-11|
|          2014-08-07|
|          2012-08-16|
|This product is n...|
+--------------------+
```

To make casting this column into an Integer possible and make it usable, a UDF was defined and later applied, taking "non-castable" data into account and masking those records with the integer (-1), so that they can be filterable later.

```scala
    // UDF to convert string star_rating to int safely
    val udfInt = udf((s: String) => if (s.forall(Character.isDigit)) s.toInt else -1)
    
    // Safely casting star_rating from string to int
    val intRatingAmazonDS = amazonDS.withColumn("int_star_rating",
      when(col("star_rating").isNotNull, udfInt(col("star_rating"))).otherwise(lit(null)))
```

Lastly, nulls per each column where totalized. Null count analysis shows that many columns present missing data, but compared to the total amount of rows, it is minimal,  , which indicates that dropping them of wouldn't hurt the analysis.

```scala
    // Custom function that counts null values for each column_name in array
    def countCols(columns: Array[String]) = {
      columns.map(
        c => count(when(col(c).isNull,c)).alias(c)
      )
    }
    
     // Creating DataFrame with all same columns and their null counts (contains just one row)
    val nullCountDF = intRatingAmazonDS.select(countCols(intRatingAmazonDS.columns):_*)
    nullCountDF.write
      .mode(SaveMode.Overwrite)
      .save(Paths.get(target_path, "null-count").toString)    
```

![Untitled](./Visualizations/"2.ColumnNullCount-BarChart.png")






