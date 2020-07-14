package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  /*
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

  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "path" -> "src/main/resources/data/cars.json"
    ))
    .load()

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars2.json")
    .save()

  // JSON
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd")
    .json("src/main/resources/data/cars.json")

  // CSV
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .option("header", "true")
    .option("sep", ",")
    .option("dateFormat", "MMM dd YYYY")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet (default Spark format)
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars2.parquet")

  // Text files
  spark.read
    .text("src/main/resources/data/828650 Belair to Mile End - 191125  C191125_231008.csv")
    .show()

  // Reading from a remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()
  */

  // Exercise
  val moviesSchema = StructType(Array(
    StructField("Title",StringType,true),
    StructField("Release_Date",DateType,true),
    StructField("Director",StringType,true),
    StructField("Creative_Type",StringType,true),
    StructField("Distributor",StringType,true),
    StructField("IMDB_Rating",DoubleType,true),
    StructField("IMDB_Votes",LongType,true),
    StructField("MPAA_Rating",StringType,true),
    StructField("Major_Genre",StringType,true),
    StructField("Production_Budget",LongType,true),
    StructField("Rotten_Tomatoes_Rating",LongType,true),
    StructField("Running_Time_min",LongType,true),
    StructField("Source",StringType,true),
    StructField("US_DVD_Sales",LongType,true),
    StructField("US_Gross",LongType,true),
    StructField("Worldwide_Gross",LongType,true)
  ))

  val moviesDF = spark.read
    .schema(moviesSchema)
    .option("dateFormat", "d-MMM-yy")
    .json("src/main/resources/data/movies.json")

  moviesDF.show()

  moviesDF.write
    .option("sep", "\t")
    .option("header", "true")
    .mode(SaveMode.Overwrite)
    .csv("src/main/resources/data/movies.csv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()
}
