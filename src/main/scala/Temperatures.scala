import org.apache.spark._
import org.apache.log4j._
import scala.math._

object Temperatures {

  def parseFields(line:String)={

    val fields= line.split(",")

    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f

    //return a tuple with 3 fields
    (stationId, entryType, temperature)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MinimumTemps")

    //read in lines of the csv file
    val dataSet = sc.textFile("/Users/sumanth/Documents/BigData/Courses/SparkScala/1800.csv")

    //apple map function to return a tuple
    val minTemps = {
      dataSet.map(parseFields).filter(x => x._2 == "TMIN").map(x => (x._1, x._3.toFloat)).reduceByKey((x, y) => min(x, y)).collect()
    }

    val maxTemps = {
      dataSet.map(parseFields).filter(x=> x._2 == "TMAX").map(x=> (x._1, x._3.toFloat)).reduceByKey((x,y) => max(x,y)).collect()
    }

    for(result<-minTemps){
      val station = result._1
      val temperature = result._2
      println(station, "---", f"$temperature%.2f F")
    }

    for(result<-maxTemps){
      val station = result._1
      val temperature = result._2
      println(station, "---", f"$temperature%.2f F")
    }

  }
}
