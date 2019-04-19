package thoughtworks

import org.apache.spark.sql._

object AnalyzerUtils {
  implicit class Dataframe(val dataframe: Dataset[Row]) {

    def countRows(spark: SparkSession): Long = {
      dataframe.count()
    }

    def averageOfAColumn(spark: SparkSession, columnName: String): Double = {
      import spark.implicits._
      import org.apache.spark.sql.functions.avg

      val dataset: Dataset[Double] = dataframe.select(avg(columnName)).as[Double]

      dataset.collect()(0)
    }

    def minimumOfAColumn(spark: SparkSession, columnName: String): Double = {
      import spark.implicits._
      import org.apache.spark.sql.functions.min

      val dataset: Dataset[Double] = dataframe.select(min(columnName)).as[Double]

      dataset.collect()(0)
    }

    def maximumOfAColumn(spark: SparkSession, columnName: String): Double = {
      import spark.implicits._
      import org.apache.spark.sql.functions.max

      val dataset: Dataset[Double] = dataframe.select(max(columnName)).as[Double]

      dataset.collect()(0)
    }

    def filterAColumn(spark: SparkSession, filterCondition: Column): Dataset[Row] = {
      dataframe.filter(filterCondition)
    }

    def addAColumn(spark: SparkSession, columnName: String, columnValue: Column): Dataset[Row] = {
      dataframe.withColumn(columnName, columnValue)
    }

    def dropAColumn(spark: SparkSession, columnName: String): Dataset[Row] = {
      dataframe.drop(columnName)
    }
  }
}
