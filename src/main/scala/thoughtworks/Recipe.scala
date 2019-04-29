package thoughtworks

import org.apache.spark.sql.SparkSession

object Recipe {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Analyze Recipes Data Spark App").getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val recipesDF = spark.read
      .option("inferSchema","true")
      .option("header","true")
      .json("./src/main/resources/recipes.json")

    println(s"The schema of the data is")
    recipesDF.printSchema()

    //Split the column Method into one method per row
    //Split the column Ingredients into one ingredient per row
  }
}
