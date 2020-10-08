package com.sample.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

class Readers(spark: SparkSession) {

  def readCSV(path: String, header: Boolean = true, cache: Boolean = false): Try[DataFrame] = {

    Try{
      val df = spark.read.option("header", header).csv(path)
      if (cache) df.cache
      df
    }
  }
}
