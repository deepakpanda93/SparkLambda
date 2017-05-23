package batch

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}

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

    input.foreach(println)

  }
}
