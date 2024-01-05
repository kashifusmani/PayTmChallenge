package com.paytm.conf

import org.rogach.scallop.{ScallopConf, ScallopOption}

class WeatherStatsConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val appName: ScallopOption[String] = opt[String](required = false, default = Some("PayTm Weather Challenge"))
  val masterUrl: ScallopOption[String] = opt[String](required = true)
  val stationPath: ScallopOption[String] = opt[String](required = true)
  val countryPath: ScallopOption[String] = opt[String](required = true)
  val dataPath: ScallopOption[String] = opt[String](required = true)
  val cleanedCountriesFileName: ScallopOption[String] = opt[String](required = false, default = Some("countrylist_cleaned.csv"))
  val year: ScallopOption[Int] = opt[Int](required = true)
  val resultOutputPath: ScallopOption[String] = opt[String](required = true)
  verify()
}
