package com.paytm.objects

case class CountryResult(countryName: String, metric_value: Double, rank: Int, metricName: String) {
  override def toString: String = s"metricName=$metricName countryName=$countryName, metric_value=$metric_value, rank=$rank"
}
