package com.paytm.preprocessing


import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.paytm.preprocessing.CountryStatsPreProcessing._
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class CountryStatsPreProcessingTest extends AnyFunSuite with DataFrameComparer {
  test("join_station_countries") {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("PayTm Weather Unit tests")
      .getOrCreate()

    val stationList = Seq(
      (1, "IN"),
      (2, "US"),
      (3, "CA")
    )

    val stationColumns = Seq(
      "STN_NO",
      "COUNTRY_ABBR"
    )

    val countryList = Seq(
      ("IN", "India"),
      ("US", "United States of America"),
      ("CA", "Canada"),
      ("FR", "France")
    )

    val countryCols = Seq(
      "COUNTRY_ABBR",
      "COUNTRY_FULL"
    )
    import spark.sqlContext.implicits._

    val stationDf = stationList.toDF(stationColumns: _*)
    val countryDf = countryList.toDF(countryCols: _*)

    val resultDf = joinStationCountries(stationDf, countryDf)

    val expectedData = Seq(
      (1, "India"),
      (2, "United States of America"),
      (3, "Canada")
    )

    val expectedSchema = Seq(
      "STN_NO",
      "COUNTRY_FULL"
    )

    val expectedDF = expectedData.toDF(expectedSchema: _*)

    assertSmallDataFrameEquality(resultDf, expectedDF)
  }
  test("filter_for_valid_dates") {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("PayTm Weather Unit tests")
      .getOrCreate()

    val weatherStats = Seq(
      (1, 20201010),
      (2, 20210205),
      (3, 202209)
    )

    val weatherCols = Seq(
      "STN---",
      "YEARMODA"
    )

    import spark.sqlContext.implicits._

    val stationDf = weatherStats.toDF(weatherCols: _*)

    val resultDf = filterForValidDates(stationDf)

    val expectedData = Seq(
      (1, 20201010),
      (2, 20210205)
    )

    val expectedDF = expectedData.toDF(weatherCols: _*)

    assertSmallDataFrameEquality(resultDf, expectedDF)
  }
  test("filter_year") {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("PayTm Weather Unit tests")
      .getOrCreate()

    val weatherStats = Seq(
      (1, 20201010),
      (2, 20200205),
      (3, 20210205),
      (4, 20190205),
      (5, 20200205)
    )

    val weatherCols = Seq(
      "STN---",
      "YEARMODA"
    )

    import spark.sqlContext.implicits._

    val stationDf = weatherStats.toDF(weatherCols: _*)

    val resultDf = filterYear(stationDf, 2020)

    val expectedData = Seq(
      (1, 20201010),
      (2, 20200205),
      (5, 20200205)
    )

    val expectedDF = expectedData.toDF(weatherCols: _*)

    assertSmallDataFrameEquality(resultDf, expectedDF)
  }
  test("join_main_df_with_station_country") {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("PayTm Weather Unit tests")
      .getOrCreate()
    import spark.sqlContext.implicits._

    val mainDfData = Seq(
      ("010260", 99999, 20190101, 26.1),
      ("010260", 99999, 20190102, 24.9),
      ("010261", 99999, 20190103, 31.7),
      ("", 99999, 20190103, 31.7)
    )

    val mainDfCols = Seq(
      "STN---",
      "WBAN",
      "YEARMODA",
      "TEMP"
    )

    val station_country_data = Seq(
      ("010260", "Canada"),
      ("010261", "India"),
      ("010262", "United States of America")
    )

    val station_country_cols = Seq(
      "STN_NO",
      "COUNTRY_FULL"
    )

    val mainDf = mainDfData.toDF(mainDfCols: _*)
    val stationCountryDf = station_country_data.toDF(station_country_cols: _*)

    val resultDf = joinMainDfWithStationCountry(mainDf, stationCountryDf)

    val expectedData = Seq(
      (99999, 20190101, 26.1, "010260", "Canada"),
      (99999, 20190102, 24.9, "010260", "Canada"),
      (99999, 20190103, 31.7, "010261", "India")
    )

    val expectedCols = Seq(
      "WBAN",
      "YEARMODA",
      "TEMP",
      "STN_NO",
      "COUNTRY_FULL"
    )

    val expectedDf = expectedData.toDF(expectedCols: _*)

    assertSmallDataFrameEquality(resultDf, expectedDf)


  }
}
