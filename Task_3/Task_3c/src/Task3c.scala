import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object Main {
  def solution(spark: SparkSession, docIds: Seq[String]) {
    import spark.implicits._

    println("Query words:")    
    for(docId <- docIds) {
      println(docId)      
    }
    val docwordIndexFilename = "Assignment_Data/docword_index.parquet"
   
    // TODO: *** Put your solution here ***
    val df = spark.read.parquet(docwordIndexFilename)
    val filteredDF = df.filter($"docId".isin(docIds.map(_.toInt): _*))
    val windowSpec = Window.partitionBy("docId").orderBy($"count".desc)

    val resultDF = filteredDF.withColumn("rank", rank().over(windowSpec))
      .filter($"rank" === 1)
      .select("docId", "word", "count")

    resultDF.show()

    resultDF.collect().foreach(row => {
      val docId = row.getAs[Int]("docId")
      val word = row.getAs[String]("word")
      val count = row.getAs[Int]("count")
      println(s"[$docId, $word, $count]")
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
      .appName("Task3c")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    // Run solution code
    solution(spark, args)
    // Stop Spark
    spark.stop()
  }
}
