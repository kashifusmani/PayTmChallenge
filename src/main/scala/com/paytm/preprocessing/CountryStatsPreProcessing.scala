package com.paytm.preprocessing


import com.paytm.schema.WeatherSchemas.{countrySchema, stationSchema}
import com.paytm.utils.FileUtils.write_to_file
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths
import scala.io.Source

object CountryStatsPreProcessing {
  private val date_field = "YEARMODA"
  val date_fmt: String = "yyyyMMdd"

  def get_station_df(spark: SparkSession, station_file_path: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .schema(stationSchema)
      .load(station_file_path)
      .dropDuplicates("STN_NO")
      .filter(col("STN_NO").isNotNull)
      .filter(col("COUNTRY_ABBR").isNotNull)
  }

  def clean_countries_file(input_file_path: String, output_file_name: String): String = {
    val cleanedCountriesFilePath = Paths.get(input_file_path).getParent.toString + "/" + output_file_name

    val source = Source.fromFile(input_file_path)
    val lines = source.getLines().toList

    // Process lines to add quotes after the first comma
    val modifiedLines = lines.head :: "\n" :: lines.tail.map { line =>
      val Array(abbr, full) = line.split(",", 2)
      s"$abbr,${'"' + full + '"'}\n"
    }
    write_to_file(cleanedCountriesFilePath, modifiedLines.mkString)
    cleanedCountriesFilePath
  }

  def get_countries_df(spark: SparkSession, countries_file_path: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .schema(countrySchema)
      .load(countries_file_path)
      .dropDuplicates("COUNTRY_ABBR")
      .filter(col("COUNTRY_ABBR").isNotNull)
      .filter(col("COUNTRY_FULL").isNotNull)
  }

  def join_station_countries(station_df: DataFrame, countries_df: DataFrame): DataFrame = {
    station_df
      .join(countries_df, Seq("COUNTRY_ABBR"))
      .select(col("STN_NO").cast(IntegerType), col("COUNTRY_FULL"))
  }

  def filter_for_valid_dates(input_df: DataFrame): DataFrame = {
    input_df
      .filter(col(date_field).isNotNull)
      .filter(length(col(date_field)) === date_fmt.length)
  }

  def filter_year(input_df: DataFrame, year: Int): DataFrame = {
    input_df
      .filter(col(date_field).substr(0, 4) === year)
  }

  def join_main_df_with_station_country(main_df: DataFrame, station_country_df: DataFrame): DataFrame = {
    main_df
      .filter(col("STN---").isNotNull)
      .join(station_country_df, main_df("STN---") === station_country_df("STN_NO"))
  }
}
