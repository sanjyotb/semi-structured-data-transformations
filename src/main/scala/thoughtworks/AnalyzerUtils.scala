package thoughtworks

import org.apache.spark.sql._

object AnalyzerUtils {
  implicit class Dataframe(val dataframe: Dataset[Row]) {

    def countRows(spark: SparkSession): Long = {
      dataframe.count()
    }

    def averageOfAColumn(spark: SparkSession, columnName: String): Double = {
      0.0
    }

    def minimumOfAColumn(spark: SparkSession, columnName: String): Double = {
      0.0
    }

    def maximumOfAColumn(spark: SparkSession, columnName: String): Double = {
      0.0
    }

    def filterAColumn(spark: SparkSession, filterCondition: Column): Dataset[Row] = {
      spark.emptyDataFrame
    }

    def addAColumn(spark: SparkSession, columnName: String, columnValue: Column): Dataset[Row] = {
      spark.emptyDataFrame
    }

    def dropAColumn(spark: SparkSession, columnName: String): Dataset[Row] = {
      spark.emptyDataFrame
    }
  }
}
