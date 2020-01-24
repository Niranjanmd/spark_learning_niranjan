import org.apache.spark.sql.SparkSession

val sc = SparkSession.builder().appName("Spark-sql")
  .master("local[*]").getOrCreate()

val dataset = sc.read
  .option("header", true)
  .csv("C:\\Niranjan\\Practice\\spark\\spark_learning_niranjan\\src\\main\\resources\\students.csv")

dataset.show()
val cnt = dataset.count()

val sub = dataset.first().getAs("subject").toString

val year = dataset.first().getAs("year").toString.toInt

println("FILE CONTAINS " + cnt +" records ")

println(sub)

dataset.filter(x => x.getAs("subject").equals("Math")).show()
