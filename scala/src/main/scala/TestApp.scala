import org.apache.spark.{SparkConf, SparkContext}

import java.util.UUID.randomUUID

object TestApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("helloSpark")
      .setMaster("local") // spark-submit or ide
    val sc = new SparkContext(conf)

    // extract
    val x = "/Users/byeonggilpark/Desktop/workspace/spark-in-action/hello.txt"
    val y = sc.textFile(x).cache() // cache RDD

    // transform
    val counts = y.flatMap(line => line.split(" "))
      .map(word => (word, 1)) // map word as 1
      .reduceByKey(_ + _)

    // load
    counts.saveAsTextFile("/Users/byeonggilpark/Desktop/workspace/spark-in-action/" + randomUUID().toString)

    sc.stop()
  }
}
