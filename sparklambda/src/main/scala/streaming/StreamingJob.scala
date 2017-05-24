package streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils._
/**
  * Created by Pramod on 5/24/2017.
  */
object StreamingJob {
  def main(args: Array[String]) : Unit = {
    val sc = SparkUtils.getSparkContext("SparkStreamingLambda")

    val batchDuration = Seconds(2)
    val ssc = new StreamingContext(sc, batchDuration)

    val inputPath = SparkUtils.isIDE match {
      case true => "file:///F:/Pramod/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
      case false => "file://vagrant/input"
    }

    val textDStream = ssc.textFileStream(inputPath)
    textDStream.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
