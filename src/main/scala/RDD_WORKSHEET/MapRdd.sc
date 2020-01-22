
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


Logger.getLogger("apache.org").setLevel(Level.WARN)

val list = List(10,3,20,56,0)

val conf = new SparkConf().setMaster("local[*]").setAppName("sparkJavatutorial")
val sc = new SparkContext(conf)


val IntRdd = sc.parallelize(list)

val SquaredRdd = IntRdd.map(x=>Math.sqrt(x))

SquaredRdd.collect()



//* Count with out count function

val count = SquaredRdd.
  map(x => if (x > 0) 1L else 0L).
  reduce((x,y)=>x+y)

