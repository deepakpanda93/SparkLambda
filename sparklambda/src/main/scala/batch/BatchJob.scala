package batch

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
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
    implicit val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val sourceFile = "file:///F:/Pramod/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv"
    val input = sc.textFile(sourceFile)

    val inputDF = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }.toDF()

    sqlContext.udf.register("UnderExposed", (pageViewCount: Long, purchaseCount: Long) => if (purchaseCount == 0) 0 else pageViewCount / purchaseCount)

    val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour") / 1000), 1).as("timestamp_hour"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")
    ).cache()

    df.registerTempTable("activity")

    val visitorsByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        |FROM activity GROUP BY product, timestamp_hour
      """.stripMargin)

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

    activityByProduct.registerTempTable("activityByProduct")

    val underExposedProducts = sqlContext.sql(
      """SELECT
        |product,
        |timestamp_hour,
        |UnderExposed(pageview_count, purchase_count) as negative_exposure
        |FROM activityByProduct
        |ORDER BY negative_exposure
        |limit 5
      """.stripMargin)

    visitorsByProduct.take(5).foreach(println)
    activityByProduct.take(5).foreach(println)
    underExposedProducts.collect().foreach(println)

  }
}
