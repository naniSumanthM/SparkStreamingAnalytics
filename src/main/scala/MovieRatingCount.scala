import org.apache.spark._
import org.apache.log4j._

object MovieRatingCount {

  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Test")
    //create RDD object
    val lines = sc.textFile("/Users/sumanth/Documents/BigData/Courses/ml-100k/u.data")
    val ratings = lines.map(x => x.toString().split("\t")(2))
    val results = ratings.countByValue()
    // Sort tuples
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)

  }
}
