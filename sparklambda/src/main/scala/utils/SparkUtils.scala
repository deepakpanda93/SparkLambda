package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Duration
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Pramod on 5/24/2017.
  */
object SparkUtils {
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkContext(appName: String) = {

    var checkpointDirectory = ""
    val conf = new SparkConf()
      .setAppName(appName)

    // Checking if running from the IDE
    if (isIDE) {
      System.setProperty("hadoop.home.dir", "C:\\winutils")
      conf.setMaster("local[*]")
      checkpointDirectory = "file:///F:/temp/"
    } else {
      checkpointDirectory = "hdfs:///lambda-pluralsight:9000/spark/checkpoint"
    }

    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(sc: SparkContext, batchDuration: Duration) = {

  }
}
