import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

// Define case classes for input data
case class VocabWord(vocabId: Int, word: String)

object Main {
  def solution(spark: SparkSession) {
    import spark.implicits._
    // Read the input data
    val vocab = spark.read.
      schema(Encoders.product[VocabWord].schema).
      option("delimiter", " ").
      csv("Assignment_Data/vocab-small.txt").
      as[VocabWord]
    val frequentDocwordsFilename = "Assignment_Data/frequent_docwords.parquet"

    // TODO: *** Put your solution here ***
    val outputCSV = "Task_4b-out"
    val frequentDocwords = spark.read.parquet(frequentDocwordsFilename)

    val pairs = frequentDocwords.as("df1").join(frequentDocwords.as("df2"), $"df1.docId" === $"df2.docId" && $"df1.vocabId" < $"df2.vocabId")
      .select($"df1.docId", $"df1.vocabId".alias("vocabId1"), $"df2.vocabId".alias("vocabId2"))

    val pairCounts = pairs.groupBy("vocabId1", "vocabId2").agg(countDistinct("docId").alias("docCount"))

    val pairsWithWords = pairCounts
      .join(vocab.as("v1"), $"vocabId1" === $"v1.vocabId")
      .join(vocab.as("v2"), $"vocabId2" === $"v2.vocabId")
      .select($"v1.word".alias("word1"), $"v2.word".alias("word2"), $"docCount")

    val orderedPairs = pairsWithWords.orderBy($"docCount".desc, $"word1", $"word2")

    orderedPairs.write
      .option("header", "false")
      .csv(outputCSV)

    orderedPairs.show()

    orderedPairs.collect().foreach(row => {
      val word1 = row.getAs[String]("word1")
      val word2 = row.getAs[String]("word2")
      val docCount = row.getAs[Long]("docCount")
      println(s"$word1, $word2, $docCount")
    })
  }

  // Do not edit the main function
  def main(args: Array[String]) {
    // Set log level
    import org.apache.log4j.{Logger,Level}
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    // Initialise Spark
    val spark = SparkSession.builder
      .appName("Task4b")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    // Run solution code
    solution(spark)
    // Stop Spark
    spark.stop()
  }
}
