package batch

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import domain._
import utils._
/**
  * Created by Pramod on 5/23/2017.
  */
object BatchJob {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtils.getSparkContext("LambdaSpark")
    implicit val sqlContext = SparkUtils.getSQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val sourceFile = "file:///vagrant/data.tsv"
    val input = sc.textFile(sourceFile)

    val inputDF = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }.toDF()

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
        .stripMargin).cache()

    // My VM name is lambda-pluralsight
    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch")

    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)
  }
}
