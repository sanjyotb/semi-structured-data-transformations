package thoughtworks

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.Matchers
import thoughtworks.AnalyzerUtils._


class AnalyzerUtilsTest extends FeatureSpecWithSpark with Matchers {

  import spark.implicits._

  feature("calculate total number of rows") {
    scenario("count the number of rows in a dataframe") {
      val testDF = Seq("row1", "row2").toDF()

      val count: Long = testDF.countRows(spark)

      count should be(2)
    }
  }

  feature("calculate average of a column in dataframe") {
    scenario("calculate the average of a column with three items in a dataframe") {
      val columnName = "aColumn"
      val testDF = Seq(1.0, 3.0, 5.0).toDF(columnName)

      val average: Double = testDF.averageOfAColumn(spark, columnName)

      average should be(3.0)
    }

    scenario("calculate the average of a column in a dataframe") {
      val columnName = "aColumn"
      val testDF = Seq(1.0, 3.0).toDF(columnName)

      val average: Double = testDF.averageOfAColumn(spark, columnName)

      average should be(2.0)
    }
  }

  feature("calculate min and max of a column in a dataframe") {
    scenario("calculate the min and max of a column with three items in a dataframe") {
      val columnName = "aColumn"
      val testDF = Seq(1.0, 3.0, 5.0).toDF(columnName)

      val minimum: Double = testDF.minimumOfAColumn(spark, columnName)
      val maximum: Double = testDF.maximumOfAColumn(spark, columnName)

      minimum should be(1.0)
      maximum should be(5.0)
    }

    scenario("calculate the min and max of a column in a dataframe") {
      val columnName = "aColumn"
      val testDF = Seq(2.0, 6.0).toDF(columnName)

      val minimum: Double = testDF.minimumOfAColumn(spark, columnName)
      val maximum: Double = testDF.maximumOfAColumn(spark, columnName)

      minimum should be(2.0)
      maximum should be(6.0)
    }
  }

  feature("apply filter on a dataframe") {
    scenario("apply basic filter on a dataframe") {
      val columnName = "aColumn"
      val testDF = Range(1, 10).toDF(columnName)

      import org.apache.spark.sql.functions.lit
      import spark.implicits._
      val actualDF: Dataset[Row] = testDF.filterAColumn(spark, testDF.col(columnName) > lit(5))

      val expectedDF = Range(6, 10).toDF(columnName)

      actualDF.except(expectedDF).count() should be(0)
      expectedDF.except(actualDF).count() should be(0)
    }
  }

  feature("add a column in dataframe") {
    scenario("adds a column in dataframe") {
      import org.apache.spark.sql.functions.lit

      val columnA = "aColumn"
      val columnB = "bColumn"
      val testDF = Seq(1.0, 3.0, 5.0).toDF(columnA)

      val actualDF = testDF.addAColumn(spark, columnB, lit("B"))

      val expectedDF = Seq((1.0, "B"), (3.0, "B"), (5.0, "B")).toDF(columnA, columnB)

      actualDF.except(expectedDF).count() should be(0)
      expectedDF.except(actualDF).count() should be(0)
    }
  }

  feature("drop a column in dataframe") {
    scenario("drops a column in dataframe") {
      val columnA = "aColumn"
      val columnB = "bColumn"
      val testDF = Seq((1.0, "a"), (3.0, "b"), (5.0, "c")).toDF(columnA, columnB)

      val actualDF = testDF.dropAColumn(spark, columnB)

      val expectedDF = Seq(1.0, 3.0, 5.0).toDF(columnA)

      actualDF.except(expectedDF).count() should be(0)
      expectedDF.except(actualDF).count() should be(0)
    }
  }
}
