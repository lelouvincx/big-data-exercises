import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def groupBalance(balance: Int): String = {
    if (balance <= 500) { // low if from -inf to 500
      "low"
    } else if (balance <= 1500) { // medium if from 501 to 1500
      "medium"
    } else { // high if from 1501 to +inf
      "high"
    }
  }

  def solution(sc: SparkContext) {
    // Load each line of the input data
    val bankdataLines = sc.textFile("Assignment_Data/bank.csv")
    // Split each line of the input data into an array of strings
    val bankdata = bankdataLines.map(_.split(";"))

    // TODO: *** Put your solution here ***
    val groupByBalance = bankdata.map(row => {
      val balance = row(5).toString.toInt
      val group = groupBalance(balance)
      (group, 1)
    })

    val sumByBalance = groupByBalance.reduceByKey(_ + _)

    // Save to text file
    sumByBalance.coalesce(1, shuffle = true).saveAsTextFile("Task_1c-out")
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task1c")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .config("spark.default.parallelism", 1)
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext)
    // Stop Spark
    spark.stop()
  }
}
