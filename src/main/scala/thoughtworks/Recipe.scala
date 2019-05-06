package thoughtworks

import org.apache.spark.sql.{DataFrame, SparkSession}

object Recipe {
  private val spark = SparkSession.builder().appName("Analyze Recipes Data Spark App").getOrCreate()

  def getRecipeDF: DataFrame ={
    spark.read
      .option("inferSchema","true")
      .option("header","true")
      .json("./src/main/resources/recipes.json")
  }

  def getUniqueIngredientsCount: Long ={
    ???
  }
}
