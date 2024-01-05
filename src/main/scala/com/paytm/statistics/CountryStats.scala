package com.paytm.statistics

import com.paytm.objects.CountryResult
import com.paytm.schema.WeatherSchemas._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CountryStats {

  private val invalidDefaults = Map(
    "TEMP" -> 9999.9,
    "WDSP" -> 999.9
  )

  def getCountryAverageMetricByRank(inputDf: DataFrame, metric: String, rank: Int, sortOrder: String, metricName: String): CountryResult = {
    val interimResult = inputDf
      .filter(col(metric) =!= invalidDefaults(metric))
      .groupBy(col("COUNTRY_FULL").as("COUNTRY_NAME_FULL"))
      .agg(avg(col(metric)).as("METRIC"))
      .orderBy(if (sortOrder == "asc") asc("METRIC") else desc("METRIC"))
      .withColumn("RANK", row_number().over(Window.orderBy(monotonically_increasing_id())))
      .filter(col("RANK") === rank)
      .select(resultSchema.map(field => col(field.name)): _*)
    //TODO: What if rank is not in the dataset.
    val firstRow = interimResult.first()

    CountryResult(
      firstRow.getAs[String]("COUNTRY_NAME_FULL"),
      firstRow.getAs[Double]("METRIC"),
      firstRow.getAs[Int]("RANK"),
      metricName
    )
  }

  def getCountryWithConsecutiveDaysOfIndicator(inputDf: DataFrame, metric: String, metricIndex: Int, metricName: String): CountryResult = {
    val tf = "metricValue"
    val group = "group"

    val windowByCountry = Window.partitionBy("COUNTRY_FULL").orderBy("YEARMODA")
    val windowByCountryAndGroup = Window.partitionBy("COUNTRY_FULL", group).orderBy("YEARMODA")

    val groupedDF = inputDf
      .filter(length(col(metric)) === metric.length)
      .withColumn(tf, split(col(metric), "").getItem(metricIndex))
      .withColumn("prev_date", lag("YEARMODA", 1).over(windowByCountry))
      .withColumn(group, when(
        (col(tf) === "1") &&
          ((datediff(col("YEARMODA"), col("prev_date")) === 1) || col("prev_date").isNull),
        0
      ).otherwise(1))
      .withColumn(group, sum(col(group)).over(windowByCountry))

      .withColumn("consecutive_ones",
        when(col(tf) === "1", sum(col(tf)).over(windowByCountryAndGroup)).otherwise(0))
      .groupBy("COUNTRY_FULL", group)
      .agg(
        max("consecutive_ones").as("max_consecutive_ones")
      )

    val maxOnes = groupedDF.agg(max("max_consecutive_ones").as("max_ones")).first().getDouble(0)

    val firstRow = groupedDF.filter(col("max_consecutive_ones") === maxOnes)
      .select("COUNTRY_FULL", "max_consecutive_ones")
      .distinct()
      .first()

    CountryResult(
      firstRow.getAs[String]("COUNTRY_FULL"),
      firstRow.getAs[Double]("max_consecutive_ones"),
      1,
      metricName
    )

  }
}
