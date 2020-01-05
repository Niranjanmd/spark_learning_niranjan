
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.spark_project.guava.collect.Iterables




Logger.getLogger("apache.org").setLevel(Level.WARN)

val list = List(
  "WARN : Tuesday 4 September 0405",
  "ERROR : Tuesday 4 September 0408",
  "FATAL : Wednesday 5 September 1620",
  "ERROR : Friday 7 September 1811",
  "WARN : Saturday 8 September 2033"
)

val conf = new SparkConf().setMaster("local[*]").setAppName("sparkJavatutorial")
val sc = new SparkContext(conf)

sc.version

val logRdd = sc.parallelize(list)

val pariredRdd = logRdd.map(value => {
//  val full =
  (value.split(":")(0),1L)
}).reduceByKey((first,second)=>first+second).collect()



val pariredRdd_grpBy = logRdd.map(value => {
  //  val full =
  (value.split(":")(0),1L)
}).groupByKey().foreach(t=>println(t._1+"has " + t._2.size +" instance "))

//pariredRdd.reduceByKey(






