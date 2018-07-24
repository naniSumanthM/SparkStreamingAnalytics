import org.apache.spark._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object CountByRatings {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "CountByRatings")

    //return a tuple
    val movieDict = sc.broadcast(loadMovieNames())

    //read in the data files
    val dataFile = sc.textFile("/Users/sumanth/Documents/BigData/Courses/ml-100k/u.data")

    //Create a new RDD object                       //(x:key, y:value(X+Y))
    val ratingsRDD = dataFile.map(x=> (x(0).toInt, 1)).reduceByKey((x,y)=> (x+y))

    //flipped RDD
    //collect is an action, and no data is returned to the driver script unless an action is invoked
    val flippedRDD = ratingsRDD.map(x=> (x._2, x._1)).sortByKey()
    val soretedMoviesWithNames = flippedRDD.map(x=> (movieDict.value(x._2), x._1)).sortByKey().collect()
    soretedMoviesWithNames.foreach(println)

  }

  def loadMovieNames():Map[Int, String]={
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames:Map[Int, String]=Map()
    val lines = Source.fromFile("/Users/sumanth/Documents/BigData/Courses/ml-100k/u.item").getLines()

    for(line <- lines){
      var fields = line.split('|')

      if(fields.length > 1){
        movieNames+=(fields(0).toInt->fields(1))
      }
    }
    return movieNames;
  }

}
