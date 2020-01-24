package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Spark_sql_Exercise extends App {

  Logger.getLogger("org.apache").setLevel(Level.WARN)

  val spark = SparkSession.builder().appName("exercise_sql").master("local[*]").getOrCreate()

  val data =  spark.read.option("header",true).csv("C:\\Niranjan\\Practice\\spark\\spark_learning_niranjan\\src\\main\\resources\\students.csv")

  data.printSchema()

  import spark.implicits._
  data.groupBy($"subject").pivot("year").agg(round(avg($"score"),2).as("Average"),round(stddev($"score")).as("Std_dev")).show()



}
