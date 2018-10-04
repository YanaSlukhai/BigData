package services

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc

class HotelsService {

  def mostPopularHotelsBetweenCouples(df: DataFrame ) ={
    df.filter("srch_adults_cnt==2")
      .select("hotel_continent", "hotel_country", "hotel_market")
      .groupBy("hotel_continent", "hotel_country", "hotel_market")
      .count()
      .orderBy(desc("count"))
      .limit(3)
  }

  def mostPopularBookedAndSearchedCountry(df: DataFrame)={
    df.filter("user_location_country==hotel_country")
      .select("hotel_country")
      .groupBy("hotel_country")
      .count()
      .orderBy(desc("count"))
      .head
      .getInt(0)
  }

  def mostInteretedButNotBooked(df: DataFrame)={
    df.filter("srch_children_cnt>0")
      .filter("is_booking==0")
      .select("hotel_continent", "hotel_country", "hotel_market")
      .groupBy("hotel_continent", "hotel_country", "hotel_market")
      .count()
      .orderBy(desc("count"))
      .limit(3)
  }






}
