import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.specs2.matcher.{Matcher, Scope}
import org.specs2.mutable.SpecificationWithJUnit
import services.HotelsService

/**
  * The  class implements Unit tests for HotelsService class
  *
  */
class HotelsServiceTest extends SpecificationWithJUnit {

  trait SparkScope extends Scope {
    val hotelService = new HotelsService

    def beTheDataset(expectedDf: DataFrame): Matcher[DataFrame] = {
      beAnEmptyDataset() ^^ { (df: DataFrame) => df.except(expectedDf) aka "dataframes difference" } and
        beAnEmptyDataset() ^^ { (df: DataFrame) => expectedDf.except(df) aka "inverted dataframes difference" }
    }

    def beAnEmptyDataset(): Matcher[DataFrame] = {
      beEqualTo(0) ^^ ((_: DataFrame).count())
    }

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("HotelsService")
      .getOrCreate()

    val df = spark.read.format("csv")
      .schema(trainSchema)
      .load("src/test/resources/ex_train.csv")

    val expectedSchema = List(
      StructField("hotel_continent", IntegerType, nullable = true),
      StructField("hotel_country", IntegerType, nullable = true),
      StructField("hotel_market", IntegerType, nullable = true),
      StructField("count", IntegerType, nullable = true)
    )

  }

  val trainSchema = new StructType(Array[StructField](
    StructField("date_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("site_name", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("posa_continent", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("user_location_country", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("user_location_region", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("user_location_city", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("orig_destination_distance", DataTypes.DoubleType, nullable = true, Metadata.empty),
    StructField("user_id", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("is_mobile", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("is_package", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("channel", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("srch_ci", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("srch_co", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("srch_adults_cnt", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("srch_children_cnt", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("srch_rm_cnt", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("srch_destination_id", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("srch_destination_type_id", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("is_booking", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("cnt", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("hotel_continent", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("hotel_country", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("hotel_market", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("hotel_cluster", DataTypes.IntegerType, nullable = true, Metadata.empty)))


  "HotelsService" should {
    "return most popular hotels for couples" in new SparkScope {

      val expectedSeq = Seq(Row(2, 50, 628, 3), Row(2, 60, 1457, 2), Row(2, 45, 675, 1))
      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedSeq),
        StructType(expectedSchema)
      )
      hotelService.mostPopularHotelsBetweenCouples(df) must beTheDataset(expectedDF)
    }

    "find the most popular country where hotels are booked and searched from the same country " in new SparkScope {
      hotelService.mostPopularBookedAndSearchedCountry(df) must beEqualTo(50)
    }

    "find top 3 hotels where people with children are interested but not booked in the end" in new SparkScope {
      val expectedSeq = Seq(Row(2, 50, 628, 3), Row(2, 60, 1457, 2), Row(2, 50, 666, 1))
      val expectedDF: DataFrame = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedSeq),
        StructType(expectedSchema)
      )
      hotelService.mostInterestedWithChildrenNotBooked(df) must beTheDataset(expectedDF)
    }

  }

}
