package com.paytm

import com.paytm.conf.WeatherStatsConf
import com.paytm.preprocessing.CountryStatsPreProcessing._
import com.paytm.schema.WeatherSchemas._
import com.paytm.statistics.CountryStats._
import com.paytm.utils.FileUtils.{delete_countries_file, write_to_file}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object PayTmWeatherChallengeRunner extends App {
  val conf = new WeatherStatsConf(args)
  val spark = SparkSession.builder()
    .master(conf.masterUrl())
    .appName(conf.appName())
    .getOrCreate()
  //TODO: Check run config to be able to make it run on cluster in distributed mode, set deploy mode, etc

  // TODO: Test functions with unit tests
  // TODO: Find more question on Glassdoor
  // TODO: Also check if 2018 Dec has any data for 2019, and 2020 Jan has any data for 2019. Mention this in README
  //TODO: Tell them that configuration can be used to toggle the metrics to be calculated.
  //TODO: Add in README, logger configuration to be used
  //TODO: Probably missing a few laven dependencies to be able to run on a cluster
  //TODO: Readme add some methods can be refactored to make better unit testing

  val station_df = get_station_df(spark, conf.stationPath())

  val cleanedCountriesFilePath = clean_countries_file(conf.countryPath(), conf.cleanedCountriesFileName())

  val country_df = get_countries_df(spark, cleanedCountriesFilePath)

  val station_country_df = join_station_countries(station_df, country_df)

  val weather_year = spark.read.format("csv").option("header", "true").schema(temperatureSchema)
    .load(conf.dataPath() + conf.year())

  val weather_year_valid_dates = filter_for_valid_dates(weather_year)

  val weather_year_filtered = filter_year(weather_year_valid_dates, conf.year())

  val data_with_country = join_main_df_with_station_country(weather_year_filtered, station_country_df)

  val hottest_mean_temp = get_country_average_metric_by_rank(data_with_country, "TEMP", 1, "desc", "Hottest Mean Temperature")
  val second_highest_wind_speed = get_country_average_metric_by_rank(data_with_country, "WDSP", 2, "desc", "Second Highest Wind Speed")

  val weather_all = join_main_df_with_station_country(
    spark.read.format("csv").option("header", "true").schema(temperatureSchema).load(conf.dataPath() + "**"),
    station_country_df
  )
  val weather_valid_dates = filter_for_valid_dates(weather_all)
    .withColumn("YEARMODA", to_date(col("YEARMODA").cast(StringType), date_fmt))

  val country_with_most_tornadoes = get_country_with_consecutive_days_indicators(weather_valid_dates, "FRSHTT", 5, "Most Tornadoes")
  write_to_file(conf.resultOutputPath(),
    hottest_mean_temp.toString + "\n" + second_highest_wind_speed.toString + "\n" + country_with_most_tornadoes.toString + "\n")
  //TODO: Ideally I would like to write the result write after calculation.
  delete_countries_file(cleanedCountriesFilePath)

  /* val countries = Seq(
     ("20230101", "CountryA", "000000"),
     ("20230102", "CountryA", "000001"),
     ("20230103", "CountryA", "000000"),
     ("20230104", "CountryA", "000001"),
     ("20230105", "CountryA", "000001"),
     ("20230106", "CountryA", "000001"),
     ("20230107", "CountryA", "000000"),
     ("20230108", "CountryA", "000001"),
     ("20230109", "CountryA", "000000"),

     ("20240101", "CountryB", "000001"),
     ("20240102", "CountryB", "000000"),
     ("20240103", "CountryB", "000001"),
     ("20240104", "CountryB", "000001"),
     ("20240105", "CountryB", "000000"),
     ("20240106", "CountryB", "000000"),
     ("20240107", "CountryB", "000000"),
     ("20240108", "CountryB", "000000"),

     ("20240201", "CountryA", "000001"),
     ("20240202", "CountryA", "000001"),
     ("20240203", "CountryA", "000001"),
     ("20240204", "CountryA", "000000"),
     ("20240205", "CountryA", "000000"),
     ("20240206", "CountryA", "000000"),
     ("20240207", "CountryA", "000000"),
     ("20240208", "CountryA", "000000"),

     ("20220201", "CountryD", "000000"),
     ("20220202", "CountryD", "000001"),
     ("20220203", "CountryD", "000001"),
     ("20220204", "CountryD", "000000"),
     ("20220205", "CountryD", "000001"),
     ("20220206", "CountryD", "000000"),
     ("20220207", "CountryD", "000000"),
     ("20220208", "CountryD", "000000")

   )
   import spark.sqlContext.implicits._
   val countryColumns = Seq("YEARMODA", "COUNTRY_FULL", "FRSHTT")
   val countryDf = countries.toDF(countryColumns: _*)
     .withColumn("YEARMODA", to_date(col("YEARMODA"), "yyyyMMdd"))

   get_country_with_consecutive_days_indicators(countryDf, "FRSHTT", 5).show()*/


}