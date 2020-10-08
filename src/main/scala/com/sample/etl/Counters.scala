package com.sample.etl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Counters(spark: SparkSession) {

  import spark.implicits._

  def countDuplicates(column: String, sourceDf: DataFrame): DataFrame = {
    sourceDf.groupBy(col(column))
      .count()
      .filter($"count" > 1)
  }

  def countNullValues(column: String, sourceDf: DataFrame): Long = {
    sourceDf.filter(sourceDf(column).isNull
    || sourceDf(column) === "Unknown"
    || sourceDf(column) === "Undefined"
    || sourceDf(column).isNaN
    ).count()
  }

  //Need proper scaladoc for the methods
  def countUniqueValues(sourceDf: DataFrame): DataFrame = {
    sourceDf.select(sourceDf
      .columns
      .map(column => approx_count_distinct(col(column)).alias(column)): _*)
  }

  //I need to include column name
  def findMostCommonValue(column: String, sourceDf: DataFrame): DataFrame = {
    sourceDf.groupBy(col(column))
      .count()
      .groupBy()
      .max()
  }
}
