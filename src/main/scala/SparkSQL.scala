import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQL {
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)

  def mapper(line:String): Person = {
    val fields = line.split(',')

    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val lines = spark.sparkContext.textFile("/Users/sumanth/Documents/BigData/Courses/SparkScala/fakefriends.csv")
    val people = lines.map(mapper)

    import spark.implicits._
    val schemaPeople = people.toDS
    schemaPeople.printSchema()
    schemaPeople.createOrReplaceTempView("people")

    val all = spark.sql("Select age, count(*) as ageCount from people group by age order by age")
    val result = spark.sql("select age from people").collect().foreach(println)
    val results = all.collect()
    results.foreach(println)

    spark.stop()
  }
}