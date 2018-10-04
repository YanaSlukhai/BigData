package controllers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import services.HotelsService



object Controller {
  val topHotels = new HotelsService

  def  main(args: Array[String]): Unit = {
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

    topHotels.mostPopularHotelsBetweenCouples(df);

  }

}
