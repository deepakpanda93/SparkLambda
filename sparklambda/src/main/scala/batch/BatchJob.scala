package batch

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import domain._
/**
  * Created by Pramod on 5/23/2017.
  */
object BatchJob {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("LambdaSpark")

    if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")) {
      System.setProperty("hadoop.home.dir", "C:\\winutils")
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)

    val sourceFile = "file:///F:/Pramod/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv"
    val input = sc.textFile(sourceFile)

    val inputRDD = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }

    // Keyby creates a pair Rdd from an RDD while the key is what is provided and the value is the original value
    // keyedByProduct is of type ((product, timestamp), Activity)
    val keyedByProduct = inputRDD.keyBy( a => (a.product, a.timestamp_hour)).cache()

    // Visitors per product

    val visitorsPerProduct = keyedByProduct
      .mapValues( a => a.visitor )
      .distinct()
      .countByKey()

    val activityByProduct = keyedByProduct
      .mapValues{ a =>
         a.action match {
           case "purchase" => (1, 0, 0)
           case "add_to_cart" => (0, 1, 0)
           case "pageview" => (0, 0, 1)
         }
      }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

    activityByProduct.foreach(println)
  }
}
