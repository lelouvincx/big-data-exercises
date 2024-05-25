import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Main {
  def solution(spark: SparkSession, queryWords: Seq[String]) {
    import spark.implicits._

    println("Query words:")
    for(queryWord <- queryWords) {
      println(queryWord)
    }
    val docwordIndexFilename = "Assignment_Data/docword_index.parquet"    
    
    // TODO: *** Put your solution here ***
    val df = spark.read.parquet(docwordIndexFilename)
    val filteredDF = df.filter($"word".isin(queryWords: _*))
    val windowSpec = Window.partitionBy("word").orderBy($"count".desc)

    val resultDF = filteredDF.withColumn("rank", rank().over(windowSpec))
      .filter($"rank" === 1)
      .select("word", "docId")

    resultDF.show()

    resultDF.collect().foreach(row => {
      val word = row.getAs[String]("word")
      val docId = row.getAs[Int]("docId")
      println(s"[$word, $docId]")
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
      .appName("Task3d")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark, args)
    // Stop Spark
    spark.stop()
  }
}
