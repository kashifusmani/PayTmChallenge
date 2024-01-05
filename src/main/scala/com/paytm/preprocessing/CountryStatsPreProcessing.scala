package com.paytm.preprocessing


import com.paytm.schema.WeatherSchemas.{countrySchema, stationSchema}
import com.paytm.utils.FileUtils.writeToFile
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths
import scala.io.Source

/**
 * This class is meant for functions that are used to perform any
 * preprocessing(filtering invalid values, data preparation, etc)
 * prior to actual usage of the data for calculating metrics
 */
object CountryStatsPreProcessing {
  private val dateField = "YEARMODA"
  val dateFmt: String = "yyyyMMdd"

  def getStationDf(spark: SparkSession, stationFilePath: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .schema(stationSchema)
      .load(stationFilePath)
      .dropDuplicates("STN_NO")
      .filter(col("STN_NO").isNotNull)
      .filter(col("COUNTRY_ABBR").isNotNull)
  }

  /**
   * Clean the input countries file and write the output to a new file (in same directory as original file)
   * so that all country names are surrounded by quotes.
   *
   * @param inputFilePath Path to existing countries file
   * @param outputFileName  Name of the output file
   * @return path of new file
   */
  def cleanCountriesFile(inputFilePath: String, outputFileName: String): String = {
    val cleanedCountriesFilePath = Paths.get(inputFilePath).getParent.toString + "/" + outputFileName

    val source = Source.fromFile(inputFilePath)
    val lines = source.getLines().toList

    // Process lines to add quotes after the first comma
    val modifiedLines = lines.head :: "\n" :: lines.tail.map { line =>
      val Array(abbr, full) = line.split(",", 2)
      s"$abbr,${'"' + full + '"'}\n"
    }
    writeToFile(cleanedCountriesFilePath, modifiedLines.mkString)
    cleanedCountriesFilePath
  }

  def getCountriesDf(spark: SparkSession, countriesFilePath: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .schema(countrySchema)
      .load(countriesFilePath)
      .dropDuplicates("COUNTRY_ABBR")
      .filter(col("COUNTRY_ABBR").isNotNull)
      .filter(col("COUNTRY_FULL").isNotNull)
  }

  def joinStationCountries(stationDf: DataFrame, countriesDf: DataFrame): DataFrame = {
    stationDf
      .join(countriesDf, Seq("COUNTRY_ABBR"))
      .select(col("STN_NO").cast(IntegerType), col("COUNTRY_FULL"))
  }

  def filterForValidDates(inputDf: DataFrame): DataFrame = {
    inputDf
      .filter(col(dateField).isNotNull)
      .filter(length(col(dateField)) === dateFmt.length)
  }

  def filterYear(inputDf: DataFrame, year: Int): DataFrame = {
    inputDf
      .filter(col(dateField).substr(0, 4) === year)
  }

  def joinMainDfWithStationCountry(mainDf: DataFrame, stationCountryDf: DataFrame): DataFrame = {
    mainDf
      .filter(col("STN---").isNotNull)
      .join(stationCountryDf, mainDf("STN---") === stationCountryDf("STN_NO"))
      .drop("STN---")
  }
}
