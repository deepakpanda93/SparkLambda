package streaming

import akka.actor.FSM.CurrentState
import domain.{Activity, ActivityByProduct, VisitorsByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{add_months, from_unixtime}
import org.apache.spark.streaming._
import utils._
import com.twitter.algebird.{HyperLogLog, HyperLogLogMonoid}
import functions._
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

      // The dstream from the input path
      val textDStream = ssc.textFileStream(inputPath)

      // Transforming the dstream into Activity records
      val activityStream = textDStream.transform( input => {
        input.flatMap{ line =>
          val record = line.split("\\t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (record.length == 7)
            Some(Activity(record(0).toLong, record(1), record(2), record(3), record(4), record(5), record(6)))
          else
            None
        }
      } ).cache()


      val activityStateSpec = StateSpec.function(mapActivityStateFunc).timeout(Minutes(120))

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
          .map{ r => ((r.getString(0), r.getLong(1)),
            ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
          ) }
      }).mapWithState(activityStateSpec)

      // Unique Visitors by product

      val visitorStateSpec =
        StateSpec
        .function(functions.mapVisitorsStateFunc)
        .timeout(Minutes(120))

      val hll = new HyperLogLogMonoid(12)
      val statefulVisitorsByProduct = activityStream.map( a => {
        ((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
      }).mapWithState(visitorStateSpec)

      val activityStateSnapShot = statefulActivityByProduct.stateSnapshots()
      activityStateSnapShot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4)
        )
        .foreachRDD(rdd => rdd.map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
          .toDF().registerTempTable("ActivityByProduct"))

      val visitorStateSnapShot = statefulVisitorsByProduct.stateSnapshots()
      visitorStateSnapShot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4)
        )
        .foreachRDD(rdd => rdd.map(
        sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
          .toDF().registerTempTable("VisitorsByProduct")
      )


        /*.updateStateByKey((newItemsPerKey: Seq[ActivityByProduct], currentState: Option[(Long, Long, Long, Long)]) => {
        // Checking whether the state contains data for the key, if not then setting it to a default 0
        var (purchase_count, add_to_cart_count, page_view_count, last_seen_time) = currentState.getOrElse((0L, 0L, 0L, System.currentTimeMillis()))
        // The result that is going to be returned
        var result: Option[(Long, Long, Long, Long)] = null

        // Check if the new items for the key are empty
        if(newItemsPerKey.isEmpty){
          if(System.currentTimeMillis() - last_seen_time > 30000 + 2000){
            // If the last seen time is more that 30 seconds + batch time ago, invalidate the key by returning None
            result = None
          }else{
            // If you have seen the key previously before the expiry time, then return the previous(current) state
            result = Some((purchase_count, add_to_cart_count, page_view_count, last_seen_time))
          }
        }else{
          // If you have found some items in the state then iterate through the items and apply the logic present in the state function
          newItemsPerKey.foreach( a => {
            purchase_count += a.purchase_count
            add_to_cart_count += a.add_to_cart_count
            page_view_count += a.page_view_count
          })

          result = Some((purchase_count, add_to_cart_count, page_view_count, System.currentTimeMillis()))
        }

        result

      })*/

      ssc
    }
    val ssc = SparkUtils.getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
