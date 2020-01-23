import org.apache.spark.sql.SparkSession

val sc = SparkSession.builder().appName("Spark-sql")
  .master("local[*]").getOrCreate()

val dataset = sc.read
  .option("header", true)
  .csv("E:\\BDF\\sparkdata\\resources_sfj\\exams\\students.csv")

dataset.show()
val cnt = dataset.count()

val sub = dataset.first().getAs("subject").toString

val year = dataset.first().getAs("year").toString.toInt

println("FILE CONTAINS " + cnt +" records ")

println(sub)

dataset.filter(x => x.getAs("subject").equals("Math")).show()
