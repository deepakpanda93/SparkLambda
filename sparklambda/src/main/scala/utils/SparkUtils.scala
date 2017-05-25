package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
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

  def getStreamingContext(streamingApp: (SparkContext, Duration) => StreamingContext, sc: SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
        // So if there is some checkpoint dir, then get the streaming context from the checkpoint. If the checkpoint is invalidated, then provide a function to
        // Create the streaming context
      case Some(checkPointDir) => StreamingContext.getActiveOrCreate(checkPointDir, creatingFunc, sc.hadoopConfiguration, true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach( cp => ssc.checkpoint(cp))
    ssc
  }
}
