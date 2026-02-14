import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

object AverageDailyConsumption {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AverageDailyConsumption")
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
    
    // Group by house and date, sum to get daily totals
    val dailyConsumption = hourlyConsumption
      .map { case (house, date, hour, consumption) => ((house, date), consumption) }
      .reduceByKey(_ + _)
    
    // Calculate average per house
    val avgDailyPerHouse = dailyConsumption
      .map { case ((house, date), total) => (house, (total, 1)) }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { case (house, (totalConsumption, numDays)) => 
        (house, totalConsumption / numDays) 
      }
    
    // Print results
    avgDailyPerHouse.collect().foreach { case (house, avg) =>
      println(f"House $house: $avg%.4f kWh per day")
    }
    
    sc.stop()
  }
}
