package com.paytm.preprocessing


import org.scalatest.funsuite.AnyFunSuite
import com.paytm.preprocessing.CountryStatsPreProcessing._
import org.apache.spark.sql.{Row, SparkSession}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions._
class CountryStatsPreProcessingTest extends AnyFunSuite with DataFrameComparer {
  test("join stationCountryDf") {
    val spark = SparkSession.builder()
      .master("local[1]") //TODO Change
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

    val resultDf = join_station_countries(stationDf, countryDf)

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


}
