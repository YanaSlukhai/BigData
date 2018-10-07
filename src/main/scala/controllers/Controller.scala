package controllers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import services.HotelsService



object Controller {
  val hotelsService = new HotelsService

  def  main(args: Array[String]): Unit = {

    object Queries extends Enumeration {
      type String = Value
      val POPULAR_HOTELS_BETWEEN_COUPLES = Value("mostPopularHotelsBetweenCouples")
      val POPULAR_BOOKED_AND_SEARCHED_COUNTRY= Value("mostPopularBookedAndSearchedCountry")
      val INTERESTED_BUT_NOT_BOOKED = Value("mostInteretedButNotBooked")
    }

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("TopHotelsForCouples")
      .getOrCreate()

    val trainSchema = new StructType(Array[StructField](
      StructField("date_time", DataTypes.StringType, true, Metadata.empty),
      StructField("site_name", DataTypes.IntegerType, true, Metadata.empty),
      StructField("posa_continent", DataTypes.IntegerType, true, Metadata.empty),
      StructField("user_location_country", DataTypes.IntegerType, true, Metadata.empty),
      StructField("user_location_region", DataTypes.IntegerType, true, Metadata.empty),
      StructField("user_location_city", DataTypes.IntegerType, true, Metadata.empty),
      StructField("orig_destination_distance", DataTypes.DoubleType, true, Metadata.empty),
      StructField("user_id", DataTypes.IntegerType, true, Metadata.empty),
      StructField("is_mobile", DataTypes.IntegerType, true, Metadata.empty),
      StructField("is_package", DataTypes.IntegerType, true, Metadata.empty),
      StructField("channel", DataTypes.IntegerType, true, Metadata.empty),
      StructField("srch_ci", DataTypes.StringType, true, Metadata.empty),
      StructField("srch_co", DataTypes.StringType, true, Metadata.empty),
      StructField("srch_adults_cnt", DataTypes.IntegerType, true, Metadata.empty),
      StructField("srch_children_cnt", DataTypes.IntegerType, true, Metadata.empty),
      StructField("srch_rm_cnt", DataTypes.IntegerType, true, Metadata.empty),
      StructField("srch_destination_id", DataTypes.IntegerType, true, Metadata.empty),
      StructField("srch_destination_type_id", DataTypes.IntegerType, true, Metadata.empty),
      StructField("is_booking", DataTypes.IntegerType, true, Metadata.empty),
      StructField("cnt", DataTypes.LongType, true, Metadata.empty),
      StructField("hotel_continent", DataTypes.IntegerType, true, Metadata.empty),
      StructField("hotel_country", DataTypes.IntegerType, true, Metadata.empty),
      StructField("hotel_market", DataTypes.IntegerType, true, Metadata.empty),
      StructField("hotel_cluster", DataTypes.IntegerType, true, Metadata.empty)));

    val df = spark.read.format("csv")
      .schema(trainSchema)
      .load(args(0))

    Queries.withName(args(0)) match
    {
      case Queries.INTERESTED_BUT_NOT_BOOKED
      => hotelsService.mostInteretedButNotBooked(df)
      case Queries.POPULAR_BOOKED_AND_SEARCHED_COUNTRY
        => hotelsService.mostPopularBookedAndSearchedCountry(df)
      case Queries.POPULAR_HOTELS_BETWEEN_COUPLES
        => hotelsService.mostPopularHotelsBetweenCouples(df)
      case _ => println("Parameter is not found")

    }

  }

}
