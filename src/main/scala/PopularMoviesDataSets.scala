import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.functions._

object PopularMoviesDataSets {
  def loadMovieNames() : Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("/Users/sumanth/Documents/BigData/Courses/ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }


  final case class Movie(movieID: Int)
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("PopularMovies")
      .master("local[*]")
      .getOrCreate()

    val lines = spark.sparkContext.textFile("/Users/sumanth/Documents/BigData/Courses/ml-100k/u.data")    
    import spark.implicits._
    val moviesDS = lines.toDS()

    val topMovieIDs = moviesDS.groupBy("movieID").count().orderBy(desc("count")).cache()
    topMovieIDs.show()

    val top10 = topMovieIDs.take(10)
    val names = loadMovieNames()

    println
    for (result <- top10) {
      println (names(result(0).asInstanceOf[Int]) + ": " + result(1))
    }
    spark.stop()
  }

}

