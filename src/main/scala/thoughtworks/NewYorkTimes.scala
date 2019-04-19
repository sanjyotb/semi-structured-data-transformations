package thoughtworks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import thoughtworks.Analyzer._

object NewYorkTimes {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Analyze New York Times - Books Data Spark App").getOrCreate()

    val nytDF = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .json("./src/main/resources/nyt2.json")
      .cache()


    val nytMergePriceDF = nytDF.mergeIntegerAndDoublePrice(spark)

    //Write code for transforming published_date column into readable format
    val nytTransformedDF = nytMergePriceDF.transformPublishedDate(spark)

    val totalNumRows = nytTransformedDF.totalBooks(spark)
    //Perform operations on NYT Books Dataset


    println("Initial Analysis of Books from New York Times Data shows: \n")
    println(s"The schema of the data is")
    nytDF.printSchema()
    println(s"The total number of books we have data about is $totalNumRows")

    //Print results of the operations performed


    val explicitSchemaDF = spark.read
      .option("header", "true")
      .schema(
        getActualSchema()
      )
      .json("./src/main/resources/nyt2.json")
      .cache()

    explicitSchemaDF.printSchema()
    explicitSchemaDF.show()

    //Try using getMissingFieldSchema() and getTypeErrorInSchema() for books dataset and check output
  }

  def getTypeErrorInSchema(): StructType = {
    StructType(
      List(
        StructField("_id",
          StructType(
            List(
              StructField("$oid", StringType, false)
            )
          ), false),
        StructField("amazon_product_url", StringType, true),
        StructField("author", StringType, true),
        StructField("bestsellers_date", StructType(
          List(
            StructField("$date", StructType(
              List(
                StructField("$numberLong", LongType, true)
              )
            ), true)
          )
        ), true),
        StructField("description", StringType, true),
        StructField("price", StructType(
          List(
            StructField("$numberDouble", DoubleType, true),
            StructField("$numberInt", IntegerType, true)
          )
        ), true),
        StructField("published_date", StructType(
          List(
            StructField("$date", StructType(
              List(
                StructField("$numberLong", LongType, true)))
            , true)
          )
        ), true),
        StructField("publisher", StringType, true),
        StructField("rank", StructType(
            List(
              StructField("$numberInt", IntegerType, true)
            )
          ), false
        ),
        StructField("rank_last_week", StructType(
          List(
            StructField("$numberInt", IntegerType, true)
          )
        ), false),
        StructField("title", StringType, true),
        StructField("weeks_on_list", StructType(
          List(
            StructField("$numberInt", IntegerType, true)
          )
        ), true)
      )
    )
  }

  def getActualSchema(): StructType = {
    StructType(
      List(
        StructField("_id",
          StructType(
            List(
              StructField("$oid", StringType, false)
            )
          ), false),
        StructField("amazon_product_url", StringType, true),
        StructField("author", StringType, true),
        StructField("bestsellers_date", StructType(
          List(
            StructField("$date", StructType(
              List(
                StructField("$numberLong", StringType, true)
              )
            ), true)
          )
        ), true),
        StructField("description", StringType, true),
        StructField("price", StructType(
          List(
            StructField("$numberDouble", StringType, true),
            StructField("$numberInt", StringType, true)
          )
        ), true),
        StructField("published_date", StructType(
          List(
            StructField("$date", StructType(
              List(
                StructField("$numberLong", StringType, true)))
            , true)
          )
        ), true),
        StructField("publisher", StringType, true),
        StructField("rank", StructType(
            List(
              StructField("$numberInt", StringType, true)
            )
          ), false
        ),
        StructField("rank_last_week", StructType(
          List(
            StructField("$numberInt", StringType, true)
          )
        ), false),
        StructField("title", StringType, true),
        StructField("weeks_on_list", StructType(
          List(
            StructField("$numberInt", StringType, true)
          )
        ), true)
      )
    )
  }

  def getMissingFieldSchema(): StructType = {
    StructType(
      List(
        StructField("_id",
          StructType(
            List(
              StructField("$oid", StringType, false)
            )
          ), false),
        StructField("amazon_product_url", StringType, true),
        StructField("author", StringType, true),
        StructField("bestsellers_date", StructType(
          List(
            StructField("$date", StructType(
              List(
                StructField("$numberLong", StringType, true)
              )
            ), true)
          )
        ), true),
        StructField("description", StringType, true),
        StructField("price", StructType(
          List(
            StructField("$numberDouble", StringType, true),
            StructField("$numberInt", StringType, true)
          )
        ), true),
        StructField("published_date", StructType(
          List(
            StructField("$date", StructType(
              List(
                StructField("$numberLong", StringType, true)))
            , true)
          )
        ), true),
        StructField("publisher", StringType, true),
        StructField("rank", StructType(
            List(
              StructField("$numberInt", StringType, true)
            )
          ), false
        ),
        StructField("rank_last_week", StructType(
          List(
            StructField("$numberInt", StringType, true)
          )
        ), false),
        //StructField("title", StringType, true),
        StructField("weeks_on_list", StructType(
          List(
            StructField("$numberInt", StringType, true)
          )
        ), true)
      )
    )
  }
}
