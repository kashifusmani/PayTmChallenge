package com.paytm.schema

import org.apache.spark.sql.types._

object WeatherSchemas {
  val temperatureSchema: StructType = StructType(
    Array(
      StructField("STN---", IntegerType, nullable = true),
      StructField("WBAN", IntegerType, nullable = true),
      StructField("YEARMODA", IntegerType, nullable = true),
      StructField("TEMP", DoubleType, nullable = true),
      StructField("DEWP", DoubleType, nullable = true),
      StructField("SLP", DoubleType, nullable = true),
      StructField("STP", DoubleType, nullable = true),
      StructField("VISIB", DoubleType, nullable = true),
      StructField("WDSP", DoubleType, nullable = true),
      StructField("MXSPD", DoubleType, nullable = true),
      StructField("GUST", DoubleType, nullable = true),
      StructField("MAX", StringType, nullable = true),
      StructField("MIN", StringType, nullable = true),
      StructField("PRCP", StringType, nullable = true),
      StructField("SNDP", DoubleType, nullable = true),
      StructField("FRSHTT", IntegerType, nullable = true))
  )
  val stationSchema: StructType = StructType(
    Array(
      StructField("STN_NO", StringType, nullable = true),
      StructField("COUNTRY_ABBR", StringType, nullable = true)
  ))

  val countrySchema: StructType = StructType(
    Array(
      StructField("COUNTRY_ABBR", StringType, nullable = true),
      StructField("COUNTRY_FULL", StringType, nullable = true)
  ))

  val resultSchema: StructType = StructType(
    Array(
      StructField("COUNTRY_NAME_FULL", StringType, nullable = false),
      StructField("METRIC", DoubleType, nullable = false),
      StructField("RANK", IntegerType, nullable = false)
  ))
}
