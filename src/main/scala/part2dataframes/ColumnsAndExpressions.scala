package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("Columns & Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsDF = spark.read
    .schema(carsSchema)
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  val firstColumn = carsDF.col("Name")
}
