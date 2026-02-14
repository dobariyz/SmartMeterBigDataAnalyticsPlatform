import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

object MaxHourlyConsumption {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MaxHourlyConsumption")
    val sc = new SparkContext(conf)
    
    // Load consumption data from HDFS
    val rawData = sc.textFile("hdfs:///Consumption/Batch/2016/*.csv")
    
    // Remove header and parse data
    val parsedData = rawData
      .filter(!_.contains("LOG_ID"))
      .map(line => {
        val fields = line.split("\t")
        (fields(0), fields(1), fields(2), fields(3), fields(4).toDouble)
      })
      .sortBy(x => (x._1, x._2, x._3, x._4))
    
    // Calculate hourly consumption using mapPartitions
    val hourlyConsumption = parsedData.mapPartitions(iter => {
      val result = ArrayBuffer[(String, String, String, Double)]()
      if (iter.hasNext) {
        var prev = iter.next()
        while (iter.hasNext) {
          val curr = iter.next()
          if (curr._2 == prev._2) {
            val consumption = curr._5 - prev._5
            val hour = curr._4.substring(0, 2)
            result += ((curr._2, curr._3, hour, consumption))
          }
          prev = curr
        }
      }
      result.iterator
    })
    
    // Find global maximum
    val maxRecord = hourlyConsumption
      .reduce((a, b) => if (a._4 > b._4) a else b)
    
    // Extract and format result
    val (house, date, hour, maxConsumption) = maxRecord
    
    println(f"Maximum hourly consumption: $maxConsumption%.4f kWh")
    println(s"Occurred at: House $house, Date $date, Hour $hour:00")
    
    sc.stop()
  }
}
