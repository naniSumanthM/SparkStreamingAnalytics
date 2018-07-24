import org.apache.spark._
import org.apache.log4j._

object MaxTemperatures {

  def parseLine(line:String)= {
    val fields = line.split(",")
    val day = fields(1)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (day, entryType, temperature)
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR

    val sc = new SparkContext("local[*]", "MinTemperatures")
    val lines = sc.textFile("/Users/sumanth/Documents/BigData/Courses/SparkScala/1800.csv")
    val parsedLines = lines.map(parseLine)
    val maxTemps = parsedLines.filter(x => x._2 == "PRCP")
    val stationTemps = maxTemps.map(x => (x._1, x._3.toFloat))
    println(stationTemps.maxBy(_._2))

    val maxPercp = stationTemps.reduceByKey((x,y) => max(x,y))
    val results = maxPercp.collect()

    for (result <- results.sorted) {
    val station = result._1
    val temp = result._2
    val formattedTemp = f"$temp%.2f F"
    println(s"$station Max Percipitation: $formattedTemp")
    }
  }
}