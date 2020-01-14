import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

Logger.getLogger("org.apache").setLevel(Level.INFO)
val conf = new SparkConf().setMaster("local[*]").setAppName("sparkExercise")
val sc = new SparkContext(conf)
sc.setLogLevel("ERROR")

val viewsRdd = sc.textFile("E:\\BDF\\sparkdata\\resources_sfj\\viewing figures\\views-*")

val courseRdd = sc.textFile("E:\\BDF\\sparkdata\\resources_sfj\\viewing figures\\titles.csv")

val chapterRdd = sc.textFile("E:\\BDF\\sparkdata\\resources_sfj\\viewing figures\\chapters.csv")

//courceID,Numberofchapter
val chapterInEachCourse = chapterRdd.map(chap => {
  val course =chap.split(",")
  (course(1).toInt, 1L)
}).reduceByKey((x, y) => x + y)

chapterInEachCourse.foreach(println(_))


val userView = viewsRdd.map(x=>{
 val line = x.split(",")
  (line(0).toInt , line(1).toInt)
}).distinct()

val chapter = chapterRdd.map(x=>
{
  val line = x.split(",")
  (line(0).toInt,line(1).toInt)
})


val courseView = userView.map(x=>x.swap)
//userid,courseid,chapid
courseView.join(chapter).map(x=>(x.x._2._1,x._2._2,x._1))
  .filter(x=>x._1==302)
//  .take(100).foreach(println(_))


//get the count of chapter watched by each user in each cource
//userid,courseid,CompletedChapter
val courseWatchCount = courseView.join(chapter).map(x=>((x.x._2._1,x._2._2),1L))
//  .filter(x=>x._1._1==302)
  .reduceByKey((x,y)=>x+y)
  .map(x => (x._1._1,x._1._2,x._2))

//  .take(100).foreach(println(_))
//course,user,completedChapCOunt,Totalchapcount
val courseView_Completed = courseWatchCount
  .map(x=>(x._2,(x._1,x._3)))
  .join(chapterInEachCourse)
  .map(x=>(x._1,x._2._1._1,x._2._1._2.toFloat,x._2._2))

//.take(100).foreach(println(_))
//course,user,completedChapCOunt,Totalchapcount,percent_completed,score

//course,score
val courseView_withPercentage = courseView_Completed
  .map(x => (x._1,Math.round(x._3/x._4 * 100)))
  .mapValues(x=>{
    if ( x>90) 10L
    else if (x>50 && x<90) 4L
    else if (x>25 && x<50) 2L
    else 0L
  })
//  .take(100).foreach(println(_))
println("percentage -------")

//courseId,score
val courseWithScore = courseView_withPercentage
  .reduceByKey((x,y)=>x+y)
  .sortByKey(false)
  .take(100)
  .foreach(println(_))


//courseWithScore.map(x=>((x._1,x._6),1L))
//  .reduceByKey((x,y)=>x+y)
//  .sortBy(x=>x._2,false)
//  .take(20)
//  .foreach(println(_))
//
//val a = 7
//val b = 10
//println(a/b)




