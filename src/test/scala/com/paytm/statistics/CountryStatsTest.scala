package com.paytm.statistics

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.paytm.statistics.CountryStats._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}
import org.scalatest.funsuite.AnyFunSuite

class CountryStatsTest extends AnyFunSuite with DataFrameComparer {
  test("get_country_average_metric_by_rank") {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("PayTm Weather Unit tests")
      .getOrCreate()
    import spark.sqlContext.implicits._

    val temp_field_name = "TEMP"
    val metric_name = "Country with highest mean temperature"
    val rank = 1

    /**
     * Country, Average Mean Temperature
     * Canada 44.55
     * India  44.0
     * United States Of America 15.50
     * Djibouti 10.0
     */
    val input_data = Seq(
      ("Canada", 45.1),
      ("United States Of America", 30.0),
      ("India", 44.0),
      ("India", 9999.9), //Invalid value
      ("Canada", 44.0),
      ("Djibouti", 10.0),
      ("Canada", 9999.9), //Invalid value
      ("United States Of America", -1.0)
    )

    val inputCols = Seq(
      "COUNTRY_FULL",
      temp_field_name
    )
    val inputDf = input_data.toDF(inputCols: _*)
    val result = getCountryAverageMetricByRank(inputDf, temp_field_name, rank, "desc", metric_name)
    assert(result.countryName === "Canada")
    assert(result.metricName === metric_name)
    assert(result.rank === 1)
    assert(result.metricValue === 44.55)

    val result_2 = getCountryAverageMetricByRank(inputDf, temp_field_name, 2, "desc", metric_name)
    assert(result_2.countryName === "India")
    assert(result_2.metricValue === 44.0)

    val result_3 = getCountryAverageMetricByRank(inputDf, temp_field_name, 3, "desc", metric_name)
    assert(result_3.countryName === "United States Of America")
    assert(result_3.metricValue === 14.50)

    val result_4 = getCountryAverageMetricByRank(inputDf, temp_field_name, 4, "desc", metric_name)
    assert(result_4.countryName === "Djibouti")
    assert(result_4.metricValue === 10.0)

    //Test for Ascending order
    val result_5 = getCountryAverageMetricByRank(inputDf, temp_field_name, 1, "asc", "Lowest Mean Temperature")
    assert(result_5.countryName === "Djibouti")
    assert(result_5.metricValue === 10.0)
  }

  test("get_country_with_consecutive_days_indicators") {

    val inputData = Seq(
      /** Number of consecutive days in each country where Tornado or Funnel Cloud = 1
       * Canada -> 1, 3, 1, 3
       * India -> 1, 2
       * Djibouti -> 2, 1
       */

      ("20230101", "Canada", "000000"),
      ("20230102", "Canada", "000001"),
      ("20230103", "Canada", "000000"),
      ("20230104", "Canada", "000001"),
      ("20230105", "Canada", "000001"),
      ("20230106", "Canada", "000001"),
      ("20230107", "Canada", "00001"),//Invalid Entry
      ("20230108", "Canada", "000001"),
      ("20230109", "Canada", "000001"),

      ("20240101", "India", "000001"),
      ("20240102", "India", "00001"), //Invalid Entry
      ("20240103", "India", "000001"),
      ("20240104", "India", "000001"),
      ("20240105", "India", "000000"),
      ("20240106", "India", "000000"),
      ("20240107", "India", "000000"),
      ("20240108", "India", "000000"),

      ("20240201", "Canada", "000001"),
      ("20240202", "Canada", "000001"),
      ("20240203", "Canada", "000001"),
      ("20240204", "Canada", "00001"),//Invalid Entry
      ("20240205", "Canada", "000000"),
      ("20240206", "Canada", "000000"),
      ("20240207", "Canada", "000000"),
      ("20240208", "Canada", "000000"),

      ("20220201", "Djibouti", "000000"),
      ("20220202", "Djibouti", "000001"),
      ("20220203", "Djibouti", "000001"),
      ("20220204", "Djibouti", "000000"),
      ("20220205", "Djibouti", "000001"),
      ("20220206", "Djibouti", "000000"),
      ("20220207", "Djibouti", "000000"),
      ("20220208", "Djibouti", "000000")
    )

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("PayTm Weather Unit tests")
      .getOrCreate()
    import spark.sqlContext.implicits._

    val countryColumns = Seq("YEARMODA", "COUNTRY_FULL", "FRSHTT")
    val inputDf = inputData.toDF(countryColumns: _*)
      .withColumn("YEARMODA", to_date(col("YEARMODA"), "yyyyMMdd"))

    val result = getCountryWithConsecutiveDaysOfIndicator(
      inputDf, "FRSHTT", 5, "Most Consecutive Days of Tornadoes or Funnel Cloud"
    )

    assert(result.countryName === "Canada")
    assert(result.metricValue === 3)
  }

}
