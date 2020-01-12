import java.util.ArrayList

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ReduceInRdd extends  App {

//  var list = new  ArrayList[Double]()
//
//  list.add(10.3)
//  list.add(20.4)
//  list.add(30.2)
//  list.add(11.3)

  Logger.getLogger("org.apache").setLevel(Level.ERROR)

  val list = List(10.2,3.6,20.4,56.3)

  val conf = new SparkConf().setMaster("local[*]").setAppName("sparkJavatutorial")
  val sc = new SparkContext(conf)

  val myRdd = sc.parallelize(list)











}
