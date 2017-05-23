package clickstream

import java.io.FileWriter

import config.Settings

import scala.util.Random

/**
  * Created by Pramod on 5/23/2017.
  */
object LogProducer extends App {
  val wlc = Settings.WebLogGen

  val Products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val Referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray
  val Visitors = (0 to wlc.visitors).map("Visitor-" + _)
  val Pages = (0 to wlc.pages).map("Page-" + _)

  val rnd = new Random()
  val filePath = wlc.filePath

  val fw = new FileWriter(filePath, true)

  val incrementTimeEvery = rnd.nextInt(wlc.records - 1) + 1

  var timeStamp = System.currentTimeMillis()
  var adjustedTimeStamp = timeStamp

  for (iteration <- 1 to wlc.records) {
    adjustedTimeStamp = adjustedTimeStamp + ((System.currentTimeMillis()-timeStamp) * wlc.timeMultiplier)
    timeStamp = System.currentTimeMillis()
    val action = iteration % (rnd.nextInt(200) + 1) match {
      case 0 => "purchase"
      case 1 => "add_to_cart"
      case _ => "pageview"
    }
    val referrer = Referrers(rnd.nextInt(Referrers.length - 1))
    val prevPage = referrer match {
      case "Internal" => Pages(rnd.nextInt(Pages.length - 1))
      case _ => ""
    }
    val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
    val page = Pages(rnd.nextInt(Pages.length - 1))
    val product = Products(rnd.nextInt(Products.length - 1))
    //Timestamp, referrer, action, prevpage, visitor, page, product
    val line = s"$adjustedTimeStamp\t$referrer\t$action\t$prevPage\t$visitor\t$page\t$product\n"
    fw.write(line)
    //Timestamp, referrer, action, prevpage, visitor, page, product

    if(iteration % incrementTimeEvery == 0) {
      println(s"Sent $iteration messages")
      val sleeping = rnd.nextInt(incrementTimeEvery * 60)

    }
  }

  fw.close()

}
