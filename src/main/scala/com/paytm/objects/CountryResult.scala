package com.paytm.objects

case class CountryResult(countryName: String, metricValue: Double, rank: Int, metricName: String) {
  override def toString: String = s"metricName=$metricName countryName=$countryName, " +
    s"metricValue=$metricValue, rank=$rank"
}
