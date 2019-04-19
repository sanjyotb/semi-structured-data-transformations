package thoughtworks

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import thoughtworks.AnalyzerUtils._

object Analyzer {

  implicit class NYTDataframe(val nytDF: Dataset[Row]) {

    def totalQuantity(spark: SparkSession): Long = {
      nytDF.countRows(spark)
    }

    def mergeIntegerAndDoublePrice(spark: SparkSession): Dataset[Row] = {
      import spark.implicits._
      val map = Map("doublePrice" -> 0.0, "intPrice" -> 0.0)
      val withNytPriceDF = nytDF
        .addAColumn(spark,"doublePrice", nytDF.col("price.$numberDouble").cast("double"))
        .addAColumn(spark,"intPrice", nytDF.col("price.$numberInt").cast("double"))
        .na
        .fill(map)
        .addAColumn(spark,"nytprice", $"doublePrice" + $"intPrice")

      withNytPriceDF
        .dropAColumn(spark, "doublePrice")
        .dropAColumn(spark, "intPrice")
    }

    def transformPublishedDate(spark: SparkSession): Dataset[Row] = {
      nytDF.addAColumn(spark,"publishedDate",
        nytDF.col("published_date.$date.$numberLong")./(1000)
          .cast(DataTypes.TimestampType)
          .cast(DataTypes.DateType)
      )
    }

    def averagePrice(spark: SparkSession): Double = {
      nytDF.averageOfAColumn(spark, "nytprice")
    }

    def minimumPrice(spark: SparkSession): Double = {
      nytDF.minimumOfAColumn(spark, "nytprice")
    }

    def maximumPrice(spark: SparkSession): Double = {
      nytDF.maximumOfAColumn(spark, "nytprice")
    }

    def totalBooksPublished(spark: SparkSession, inYear: String): Long = {
      import spark.implicits._

      val isSoldInYear = year($"publishedDate") === inYear

      nytDF.select($"publishedDate")
        .filterAColumn(spark, isSoldInYear)
        .countRows(spark)
    }
  }
}
