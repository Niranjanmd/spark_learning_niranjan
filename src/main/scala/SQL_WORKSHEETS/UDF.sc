import org.apache.spark.sql.SparkSession
import  org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("aggrigation")
  .master("local[*]").getOrCreate()


val dataset = spark.read
  .option("header", true)
  .csv("C:\\Niranjan\\Practice\\spark\\spark_learning_niranjan\\src\\main\\resources\\students.csv")




val hasPassed = (s: String)=>{
  s == "A+"
}

spark.udf.register("hasPassed",hasPassed)

dataset
  .withColumn("passed",callUDF("hasPassed",col("grade"))).show()


val squared = (s: Long) => {
  s * s
}
spark.udf.register("square", squared)




