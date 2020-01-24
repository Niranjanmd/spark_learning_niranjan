import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("aggrigation")
  .master("local[*]").getOrCreate()


val dataset = spark.read
  .option("header", true)
  .csv("C:\\Niranjan\\Practice\\spark\\spark_learning_niranjan\\src\\main\\resources\\students.csv")


dataset.printSchema()

import spark.sqlContext.implicits._

dataset.groupBy("year")
  .agg(count("student_id").as("Count"))
  .orderBy("year")
  .show()

