
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

Logger.getLogger("apache.org").setLevel(Level.WARN)

val list = List(
  "WARN: Tuesday 4 September 0405",
  "ERROR: Tuesday 4 September 0408",
  "FATAL: Wednesday 5 September 1620",
  "ERROR: Friday 7 September 1811",
  "WARN: Saturday 8 September 2033"
)

val conf = new SparkConf().setMaster("local[*]").setAppName("sparkJavatutorial")
val sc = new SparkContext(conf)


val logRdd = sc.parallelize(list)


logRdd.flatMap(x=>x.split(" ")).collect()



logRdd.map(x=>x.split(" ")).collect()