import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object DataFrames {

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

    import spark.implicits._
    val lines = spark.sparkContext.textFile("/Users/sumanth/Documents/BigData/Courses/SparkScala/fakefriends.csv")
    val people = lines.map(mapper).toDS().cache()

    println("Here is our inferred schema:")
    people.printSchema()

    println("Let's select the name column:")
    people.select("name").show()

    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).show()

    println("Group by age:")
    people.groupBy("age").count().orderBy("age").show()

    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()

    spark.stop()
  }
}