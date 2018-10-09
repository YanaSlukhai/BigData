package services

import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  * The  class implements three queries required in Spark Core HW
  *
  */
class HotelsService {

  /**
    * The  method returns most popular hotels between couples
    *
    * @param df DataFrame containing the input data
    * @return DataFrame containing three most popular hotels between couples
    */
  def mostPopularHotelsBetweenCouples(df: DataFrame): Dataset[Row] = {
    df.filter("srch_adults_cnt==2")
      .select("hotel_continent", "hotel_country", "hotel_market")
      .groupBy("hotel_continent", "hotel_country", "hotel_market")
      .count()
      .orderBy(desc("count"))
      .limit(3)
  }

  /**
    * The  method returns most popular country equal for hotel and user location
    *
    * @param df DataFrame containing the input data
    * @return Int identifier of the most popular country equal for hotel and user location
    */
  def mostPopularBookedAndSearchedCountry(df: DataFrame): Int = {
    df.filter("user_location_country==hotel_country")
      .select("hotel_country")
      .groupBy("hotel_country")
      .count()
      .orderBy(desc("count"))
      .head
      .getInt(0)
  }

  /**
    * The  method returns most popular hotels that were searched but not booked
    *
    * @param df DataFrame containing the input data
    * @return DataFrame containing three most popular hotels that were searched but not booked
    */
  def mostInterestedWithChildrenNotBooked(df: DataFrame): Dataset[Row] = {
    df.filter(s"srch_children_cnt>0")
      .filter(s"is_booking==0")
      .select("hotel_continent", "hotel_country", "hotel_market")
      .groupBy("hotel_continent", "hotel_country", "hotel_market")
      .count()
      .orderBy(desc("count"))
      .limit(3)
  }

}
