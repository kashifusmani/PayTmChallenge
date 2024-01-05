package com.paytm

import com.paytm.conf.WeatherStatsConf
import com.paytm.preprocessing.CountryStatsPreProcessing._
import com.paytm.schema.WeatherSchemas._
import com.paytm.statistics.CountryStats._
import com.paytm.utils.FileUtils.{deleteCountriesFile, writeToFile}
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

  // TODO: Also check if 2018 Dec has any data for 2019, and 2020 Jan has any data for 2019. Mention this in README
  //TODO: Tell them that configuration can be used to toggle the metrics to be calculated.
  //TODO: Add in README, logger configuration to be used
  //TODO: Probably missing a few maven dependencies to be able to run on a cluster
  //TODO: Readme add some methods can be refactored to make better unit testing
  //TODO: Ideally I would like to write the result write after calculation.


  val stationDf = getStationDf(spark, conf.stationPath())

  val cleanedCountriesFilePath = cleanCountriesFile(conf.countryPath(), conf.cleanedCountriesFileName())

  val countryDf = getCountriesDf(spark, cleanedCountriesFilePath)

  val stationCountryDf = joinStationCountries(stationDf, countryDf)

  val weatherYear = spark.read.format("csv").option("header", "true").schema(temperatureSchema)
    .load(conf.dataPath() + conf.year())

  val weatherYearValidDates = filterForValidDates(weatherYear)

  val weatherYearFiltered = filterYear(weatherYearValidDates, conf.year())

  val dataWithCountry = joinMainDfWithStationCountry(weatherYearFiltered, stationCountryDf)

  val hottestMeanTemp = getCountryAverageMetricByRank(dataWithCountry, "TEMP", 1, "desc", "Hottest Mean Temperature")
  val secondHighestWindSpeed = getCountryAverageMetricByRank(dataWithCountry, "WDSP", 2, "desc", "Second Highest Wind Speed")

  val weatherAll = joinMainDfWithStationCountry(
    spark.read.format("csv").option("header", "true").schema(temperatureSchema).load(conf.dataPath() + "**"),
    stationCountryDf
  )
  val weatherValidDates = filterForValidDates(weatherAll)
    .withColumn("YEARMODA", to_date(col("YEARMODA").cast(StringType), dateFmt))

  val countryWithMostTornadoes = getCountryWithConsecutiveDaysOfIndicator(weatherValidDates, "FRSHTT", 5, "Most Tornadoes")
  writeToFile(conf.resultOutputPath(),
    hottestMeanTemp.toString + "\n" + secondHighestWindSpeed.toString + "\n" + countryWithMostTornadoes.toString + "\n")
  deleteCountriesFile(cleanedCountriesFilePath)

}