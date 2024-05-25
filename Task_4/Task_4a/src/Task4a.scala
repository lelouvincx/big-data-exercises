import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

// Define case classes for input data
case class Docword(docId: Int, vocabId: Int, count: Int)
case class VocabWord(vocabId: Int, word: String)

object Main {
  def solution(spark: SparkSession) {
    import spark.implicits._
    // Read the input data
    val docwords = spark.read.
      schema(Encoders.product[Docword].schema).
      option("delimiter", " ").
      csv("Assignment_Data/docword-small.txt").
      as[Docword]

    val vocab = spark.read.
      schema(Encoders.product[VocabWord].schema).
      option("delimiter", " ").
      csv("Assignment_Data/vocab-small.txt").
      as[VocabWord]
    
    val frequentDocwordsFilename = "../frequent_docwords.parquet"
    val outputCSV = "Task_4a-out"

    val joinedData = docwords.join(vocab, "vocabId")
    val wordCounts = joinedData.groupBy("word").agg(sum("count").alias("total_count"))
    val frequentWords = wordCounts.filter($"total_count" >= 1000).select("word")

    val frequentDocwords = joinedData.join(frequentWords, "word").select("docId", "vocabId", "count")

    frequentDocwords.write.parquet(frequentDocwordsFilename)

    frequentDocwords.write
      .option("header", "false")
      .csv(outputCSV)

    frequentDocwords.show()
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task4a")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}