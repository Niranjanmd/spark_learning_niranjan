import org.apache.spark.sql.SparkSession

val sc = SparkSession.builder().appName("Spark-sql")
  .master("local[*]").getOrCreate()

val dataset = sc.read
  .option("header", true)
  .csv("C:\\Niranjan\\Practice\\spark\\spark_learning_niranjan\\src\\main\\resources\\students.csv")


val cnt = dataset.count()

println("FILE CONTAINS " + cnt +" records ")




