import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

object Main {
  def solution(sc: SparkContext) {

    // WARNING: Please DO NOT modify the line below. 
    // Please make sure you first copy the data to HDFS follow instructions
    // on assignment specifications.
    // The code below loads each line of the input data from HDFS.
    val bankdataLines = sc.textFile("Assignment_Data/bank-small.csv")
    
    // Split each line of the input data into an array of strings
    val bankdata = bankdataLines.map(_.split(";"))

    // Code that 
    val result = bankdata.map(line => (line(0), line(1), line(3), line(4))); 
    result.saveAsTextFile("Task_test.out")
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
