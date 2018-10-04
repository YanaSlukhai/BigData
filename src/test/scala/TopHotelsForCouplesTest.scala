import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.specs2.matcher.{Matcher, Scope}
import org.specs2.mutable.SpecificationWithJUnit
import services.HotelsService


class TopHotelsForCouplesTest extends  SpecificationWithJUnit {

  val hotelServise = new HotelsService

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

  "HotelsService" should {
    "return most popular hotels for couples" in new Scope {
      val spark = SparkSession.builder
        .master("local[*]")
        .appName("HotelsService")
        .getOrCreate()

      val df = spark.read.format("csv")
        .schema(trainSchema)
        .load("src/test/resources/ex_train.csv")


      val expectedSchema = List(
        StructField("hotel_continent", IntegerType, true),
        StructField("hotel_market", IntegerType, true),
        StructField("hotel_cluster", IntegerType, true),
        StructField("count", IntegerType, true)
      )

      val expectedSeq = Seq(Row(3,69,36,11), Row(2,680,95,9), Row(3,69,29,7))

      val expectedDF: DataFrame = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedSeq),
        StructType(expectedSchema)
      )

      hotelServise.countPopularHotelsBetweenCouples(df) must beTheDataset(expectedDF)

      def beTheDataset(expectedDf: DataFrame): Matcher[DataFrame] = {
        beAnEmptyDataset() ^^ {(df:DataFrame) => df.except(expectedDf) aka "dataframes difference"} and
          beAnEmptyDataset() ^^ {(df:DataFrame) => expectedDf.except(df) aka "inverted dataframes difference"}

      }

      def beAnEmptyDataset(): Matcher[DataFrame] = {
        beEqualTo(0) ^^ ((_:DataFrame).count())
      }

    }

    "handle error when set is empty" in new Scope {

    }
  }


}
