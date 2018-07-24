import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.SparkContext._

object MovieRecs {

  def parseNames(line: String):Option[(Int, String)]={
    var fields = line.split('\"')

    if(fields.length > 1)
    {
      return Some(fields(0).toInt, fields(1))
    } else {
      return None
    }
  }

  //return a tuple with heroId, and number of occuerences
  def countOccuerences(line:String)={
    var elements = line.split("\\s+")
    (elements(0).toInt, elements.length - 1)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "SuperHeroCounter")
    val names = sc.textFile("/Users/sumanth/Documents/BigData/Courses/SparkScala/Marvel-names.txt")
    val namesRdd = names.map(parseNames)

    //load up the superhero co-appereance data
    val lines = sc.textFile("/Users/sumanth/Documents/BigData/Courses/SparkScala/Marvel-graph.txt")
    val pairings = lines.map(countOccuerences)
    val totalFriendsByCharcater = pairings.reduceByKey((x,y)=> (x+y))
    val connectionsAndHeroId = totalFriendsByCharcater.map(x=> (x._2, x._1))
    val mostPopular = connectionsAndHeroId.max()
    println(mostPopular)
    //use that id to query for the name of the superhero
    //val mostPopularName = namesRdd.lookup(mostPopular._2)(0)
  }
}
