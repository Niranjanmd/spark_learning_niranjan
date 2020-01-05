
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


Logger.getLogger("apache.org").setLevel(Level.WARN)

val list = List(10,3,20,56,0)

val conf = new SparkConf().setMaster("local[*]").setAppName("sparkJavatutorial")
val sc = new SparkContext(conf)



val IntRdd = sc.parallelize(list)

case class IntigerWithSquareRoot(number:Int,SquareNumber:Double) {

}

val squaredRdd = IntRdd.map(x => IntigerWithSquareRoot(x,Math.sqrt(x)))

squaredRdd.collect()


val tupleSquareRdd = IntRdd.map(x => (x,Math.sqrt(x)))

tupleSquareRdd.collect()



tupleSquareRdd.reduceByKey()



