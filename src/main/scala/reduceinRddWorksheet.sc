
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


Logger.getLogger("apache.org").setLevel(Level.WARN)

val list = List(10.2,3.6,20.4,56.3)

val conf = new SparkConf().setMaster("local[*]").setAppName("sparkJavatutorial")
val sc = new SparkContext(conf)

val myRdd = sc.parallelize(list)

//reduce to find the sum
val sum = myRdd.reduce((x,y) => x+y)




/*Notes on Reduce
*

Reduce is used to apply the
reduce function takes a function as input and produce the result
for example summation, multiplication or any other function which needs to be run on
* each pair of row in dataset
*
* format - (row1,row2)=> row
*
* */


// RDD OF PERSON OBJECT
case class Person(id:Int,name:String,age:Int)

val listPerson = List(
  new Person(1,"Niranjan",27),
  new Person(2,"abhi",26),
  new Person(1,"Niranjan",28),
  new Person(2,"abhi",24),
  new Person(1,"Niranjan",28)
)

val personRdd = sc.parallelize(listPerson)

//personRdd.reduce((x,y)=>x.age+y.age)

//Can not directly apply reduce here
val personAggRdd =personRdd.map(x =>x.age).sum()


val personPair = personRdd.keyBy(_.id).reduceByKey((x,y)=>Person(x.id,x.name,(x.age+y.age)/2))

println("person with avg age")
personPair.collect()
/*extra*/

val maprdd = myRdd.map({
  x =>x+2
})

maprdd.foreach(println(_))



maprdd.collect()
print(sum)


print(1+sum)

print(sum*sum)









