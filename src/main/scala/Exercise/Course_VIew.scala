package Exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Course_VIew extends App {


  Logger.getLogger("org.apache").setLevel(Level.WARN)
  val conf = new SparkConf().setMaster("local[*]").setAppName("sparkExercise")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val viewsRdd = sc.textFile("E:\\BDF\\sparkdata\\resources_sfj\\viewing figures\\views-*")

  val courseRdd = sc.textFile("E:\\BDF\\sparkdata\\resources_sfj\\viewing figures\\titles.csv")

  val chapterRdd = sc.textFile("E:\\BDF\\sparkdata\\resources_sfj\\viewing figures\\chapters.csv")

  println(viewsRdd.getNumPartitions)

  println("views collected " + viewsRdd.count())

  println(s"number of course -" + courseRdd.count())

  val distinct_Views = viewsRdd.map(x => (x.split(",")(0), x.split(",")(1)))
    .distinct()

  val course = courseRdd.map(x => (x.split(",")(0).toInt, x.split(",")(1)))

  val chapters = chapterRdd.map(x => {
    val arr = x.split(",")
    (arr(0), arr(1))
  })

  println("distinct Views " + distinct_Views.count())

  //  distinct_Views.take(100).foreach(println(_))

  val chapterViews = distinct_Views.map(x => (x.swap))

  val courseView = chapterViews.join(chapters)

  val mostViewedRdd = courseView.map(x => x._2).map(x => (x._2.toInt, 1L))
    .reduceByKey((x, y) => x + y)


  println("most viewed course --------")
//  mostViewedRdd.take(100).foreach(println(_))


  // courseView.join(course).take(100).foreach(println(_))


  val finalResult = mostViewedRdd.join(course)
    .map(x => (x._1, x._2._2, x._2._1))
    .sortBy(x => x._3, false)

  finalResult.take(100).foreach(println(_))







}
