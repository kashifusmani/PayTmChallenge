package com.paytm.conf

import org.scalatest.funsuite.AnyFunSuite

class WeatherStatsConfTest extends AnyFunSuite {
  test("Conf is working: HappyPath") {
    val app_name = "PayTm"
    val master_url = "local[1]"
    val station_path = "/a/b/c"
    val country_path = "/x/y/z"
    val data_path = "/a/b/c/data"
    val year = "2018"
    val clean_countries_file_name = "clean_countries.csv"
    val result_path = "/path/to/result"
    val sparkProperties = "a=b,c=d,spark.driver.cores=3,spark.submit.deployMode=client"

    val args = List(
      "--app-name",
      app_name,
      "--master-url",
      master_url,
      "--station-path",
      station_path,
      "--country-path",
      country_path,
      "--data-path",
      data_path,
      "--year",
      year,
      "--cleaned-countries-file-name",
      clean_countries_file_name,
      "--result-output-path",
      result_path,
      "--spark-properties",
      sparkProperties
    )

    val conf = new WeatherStatsConf(args)
    assert(conf.appName() === app_name)
    assert(conf.masterUrl() === master_url)
    assert(conf.stationPath() === station_path)
    assert(conf.countryPath() === country_path)
    assert(conf.dataPath() === data_path)
    assert(conf.year() === year.toInt)
    assert(conf.cleanedCountriesFileName() === clean_countries_file_name)
    assert(conf.resultOutputPath() === result_path)
    assert(conf.sparkProperties() === sparkProperties)
  }
}
