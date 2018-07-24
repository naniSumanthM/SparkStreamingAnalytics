import org.apache.spark._
import org.apache.log4j._

object CustomerSpending {

  //return tuple(custId: Int, customerSpent:Float)
  def parseLine(line: String)={
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val customerSpent = fields(2).toFloat
    (customerId, customerSpent)
  }

  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "CustomerSpending")
    val lines = sc.textFile("/Users/sumanth/Documents/BigData/Courses/SparkScala/customer-orders.csv")
    val rdd = lines.map(parseLine).reduceByKey((x, y)=> (x + math.ceil(y).toInt)).sortByKey(true, 1)
    rdd.foreach(println)

  }
}
