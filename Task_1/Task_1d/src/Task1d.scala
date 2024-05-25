import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def customReorder(arr: Array[String]): Array[String] = {
    if (arr.length >= 6) {
      val job = arr(1)
      val marital = arr(2)
      val education = arr(3)
      val balance = arr(5)
      val loan = arr(7)

      Array(education, balance, job, marital, loan)
    } else {
      arr
    }
  }

  def solution(sc: SparkContext) {
    // Load each line of the input data
    val bankdataLines = sc.textFile("Assignment_Data/bank-small.csv")
    // Split each line of the input data into an array of strings
    val bankdata = bankdataLines.map(_.split(";"))

    // TODO: *** Put your solution here ***
    val reorderedBankdata = bankdata.map(customReorder)
    val groupByEducationAndBalance = reorderedBankdata.map(row => {
      val education = row(0).toString
      val balance = row(1).toInt * (-1)
      ((education, balance), row)
    })

    val sortedByEducationAndBalance = groupByEducationAndBalance.sortByKey()
    val resultBankdata = sortedByEducationAndBalance.map(_._2)

    resultBankdata.map(_.mkString(";")).saveAsTextFile("Task_1d-out")
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task1d")
      .master("local[4]")
      .config("spark.hadoop.validateOutputSpecs", "false")
      .getOrCreate()
    // Run solution code
    solution(spark.sparkContext)
    // Stop Spark
    spark.stop()
  }
}
