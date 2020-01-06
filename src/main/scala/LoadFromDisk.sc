
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.Source



Logger.getLogger("apache.org").setLevel(Level.WARN)

val conf = new SparkConf().setMaster("local[*]").setAppName("sparkJavatutorial")
val sc = new SparkContext(conf)
val filepath ="E:\\BDF\\sparkdata\\resources_sfj\\subtitles\\input-spring.txt";

val commonWordFile = "E:\\BDF\\sparkdata\\resources_sfj\\subtitles\\boringwords.txt"

val file_data =Source.fromFile(commonWordFile).mkString
//println(file_data)

def isBoaring(value:String):Boolean={
  file_data.contains(value)
}

def isNotBoring(value:String):Boolean={
  !file_data.contains(value)
}

val fileRdd = sc.textFile(filepath)

//dont consider the commenly used words like the ,to etc

fileRdd.map(x =>x.replaceAll("[^a-zA-Z\\s]","").toLowerCase())
    .take(50).foreach(println(_))

fileRdd.map(x =>x.replaceAll("[^a-zA-Z\\s]","").toLowerCase())
  .filter(x=> !x.isEmpty)
  .flatMap(x=>x.split(" "))
  .filter( x=> isNotBoring(x))
  .take(50).foreach(println(_))
//
fileRdd.map(x =>x.replaceAll("[^a-zA-Z\\s]","").toLowerCase())
  .filter(x=> !x.isEmpty)
  .flatMap(x=>x.split(" "))
  .filter( x=> isNotBoring(x))
  .map(x=>(x,1L))
  .reduceByKey((x,y)=>x+y)
  .sortBy(x=>x._2,false)
  .take(10)
  .foreach(x => println("keywords " + x._1 +" Count -" + x._2) )
