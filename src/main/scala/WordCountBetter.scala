import org.apache.spark._
import org.apache.log4j._


object WordCountBetter {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
   
    val sc = new SparkContext("local", "WordCountBetterSorted")  
    val input = sc.textFile("/Users/sumanth/Documents/BigData/Courses/SparkScala/book.txt")  
    val words = input.flatMap(x => x.split("\\W+"))
    val lowercaseWords = words.map(x => x.toLowerCase())
    val noiseWords = List("a", "an", "but", "the", "or", "you", "to", "of")
    val noiseWordsFiltered = lowercaseWords.filter(!noiseWords.contains(_))
    val wordCounts = noiseWordsFiltered.map(x => (x, 1))
    val reducedWords = wordCounts.reduceByKey((x,y)=> (x+y))
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }

  }

}

