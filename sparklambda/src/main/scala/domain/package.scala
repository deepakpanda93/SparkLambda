/**
  * Created by Pramod on 5/23/2017.
  */
package object domain {
  ////Timestamp, referrer, action, prevpage, visitor, page, product
  case class Activity(timestamp_hour: Long,
                      referrer: String,
                      action: String,
                      prevPage: String,
                      page: String,
                      visitor: String,
                      product: String,
                      inputProps: Map[String, String] = Map()
                     )
}
