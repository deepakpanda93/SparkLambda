package streaming

import akka.actor.FSM.CurrentState
import domain.{Activity, ActivityByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{add_months, from_unixtime}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import utils._
/**
  * Created by Pramod on 5/24/2017.
  */
object StreamingJob {
  def main(args: Array[String]) : Unit = {
    val sc = SparkUtils.getSparkContext("SparkStreamingLambda")
    val sqlContext = SparkUtils.getSQLContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds(2)

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      val inputPath = SparkUtils.isIDE match {
        case true => "file:///F:/Pramod/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
        case false => "file://vagrant/input"
      }

      val textDStream = ssc.textFileStream(inputPath)
      val activityStream = textDStream.transform( input => {
        input.flatMap{ line =>
          val record = line.split("\\t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (record.length == 7)
            Some(Activity(record(0).toLong, record(1), record(2), record(3), record(4), record(5), record(6)))
          else
            None
        }
      } )

      val statefulActivityByProduct = activityStream.transform( rdd => {
        val df = rdd.toDF()
        df.registerTempTable("activity")
        val activityByProduct = sqlContext.sql(
          """SELECT
            |product,
            |timestamp_hour,
            |sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
            |sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
            |sum(case when action = 'pageview' then 1 else 0 end) as pageview_count
            |FROM activity
            |GROUP BY product, timestamp_hour"""
            .stripMargin)

        activityByProduct
          .map{ r => ((r.getString(0), r.getString(1)),
            ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
          ) }
      }).updateStateByKey( (newItemsPerKey: Seq[ActivityByProduct], currentState: Option[(Long, Long, Long)]) => {
        var (purchase_count, add_to_cart_count, page_view_count) = currentState.getOrElse((0L, 0L, 0L))

        newItemsPerKey.foreach( a => {
          purchase_count += a.purchase_count
          add_to_cart_count += a.add_to_cart_count
          page_view_count += a.page_view_count
        })

        Some((purchase_count, add_to_cart_count, page_view_count))
      })

      statefulActivityByProduct.print(10)
      ssc
    }
    val ssc = SparkUtils.getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
