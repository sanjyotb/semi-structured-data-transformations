package thoughtworks

import org.apache.spark.sql.SparkSession

object Recipe {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Analyze Recipes Data Spark App").getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val recipesDF = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .json("./src/main/resources/recipes.json")

    println(s"The schema of the data is")
    recipesDF.printSchema()

    recipesDF.select($"Author", $"Name", $"Method", explode($"Method") as "Steps").show()
    recipesDF.select($"Author", $"Name", $"Ingredients", explode($"Ingredients") as "Ingredient").show()
  }
}
