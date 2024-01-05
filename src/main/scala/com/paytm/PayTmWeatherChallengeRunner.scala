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

  val stationDf = getStationDf(spark, conf.stationPath())

  val cleanedCountriesFilePath = cleanCountriesFile(conf.countryPath(), conf.cleanedCountriesFileName())

  val countryDf = getCountriesDf(spark, cleanedCountriesFilePath)

  val stationCountryDf = joinStationCountries(stationDf, countryDf)

  val weatherYear = spark.read.format("csv").option("header", "true").schema(temperatureSchema)
    .load(conf.dataPath() + conf.year())

  val weatherYearValidDates = filterForValidDates(weatherYear)

  val weatherYearFiltered = filterYear(weatherYearValidDates, conf.year())

  val dataWithCountry = joinMainDfWithStationCountry(weatherYearFiltered, stationCountryDf)

  //Now we are ready to calculate the metrics
  val hottestMeanTemp = getCountryAverageMetricByRank(
    dataWithCountry, "TEMP", 1, "desc", "Hottest Mean Temperature")

  val secondHighestWindSpeed = getCountryAverageMetricByRank(
    dataWithCountry, "WDSP", 2, "desc", "Second Highest Wind Speed")

  val weatherAll = joinMainDfWithStationCountry(
    spark.read.format("csv").option("header", "true").schema(temperatureSchema).load(conf.dataPath() + "**"),
    stationCountryDf
  )
  val weatherValidDates = filterForValidDates(weatherAll)
    .withColumn("YEARMODA", to_date(col("YEARMODA").cast(StringType), dateFmt))


  val countryWithMostTornadoes = getCountryWithConsecutiveDaysOfIndicator(
    weatherValidDates, "FRSHTT", 5, "Most Tornadoes")
  //Write the results to a file
  writeToFile(conf.resultOutputPath(),
    hottestMeanTemp.toString + "\n" + secondHighestWindSpeed.toString + "\n" + countryWithMostTornadoes.toString + "\n")
  //Delete the countries file with quotes
  deleteCountriesFile(cleanedCountriesFilePath)
}