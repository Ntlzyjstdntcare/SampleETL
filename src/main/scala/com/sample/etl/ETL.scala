package com.sample.etl

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

import scala.util.{Failure, Success, Try}

object ETL {

  val logger: Logger = Logger.getLogger(this.getClass.getName)

  val totalRowsCol = "total_rows"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Simple ETL With Report")
      .master("local[4]")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/ais.ais_data")
      .getOrCreate()

    val counters = new Counters(spark)
    val writers = new Writers(spark)
    val readers = new Readers(spark)
    val utils = new Utils(spark.sparkContext)

    //Should get from conf
    val readAndWriteData: Try[Unit] = for {
      aisDf: DataFrame <- readers.readCSV("/Users/colmginty/Downloads/aisdk_20190119.csv", cache = true)
      _ = writers.writeDfToMongo("ais_data", "overwrite", aisDf)

      totalRows: Long <- Try(aisDf.count())
      totalRowsRDD: RDD[Document] <- Try(utils.convertToRddDocument(totalRowsCol, totalRows))
      _ = writers.writeRddToMongo(totalRowsRDD, "ais", totalRowsCol)

      uniqueValues: DataFrame <- Try(counters.countUniqueValues(aisDf))
      _ = writers.writeDfToMongo("unique_values", "overwrite", uniqueValues)

      _ = iterateOverColumnsAndWrite(aisDf)
    } yield ()

    readAndWriteData match {
      case Success(_) => println("Successfully read AIS data, " +
        "and wrote it to db along with summary data")
      case Failure(exception) => println(s"Problem moving AIS data " +
        s"and/or generating summary data: $exception"); logger.error(exception)
    }

    //I should build up the collection and save it all in one go
    //I should get the params from conf
    //There must be a (much) better way to do this
    def iterateOverColumnsAndWrite(df: DataFrame): Try[Unit] = {
      Try{
        df.columns.take(1).foreach { column =>
          val duplicatesCount: DataFrame = counters.countDuplicates(column, df)
          writers.writeDfToMongo("duplicates_count", "append", duplicatesCount)

          val nullsCount: Long = counters.countNullValues(column, df)
          val nullsCountRdd: RDD[Document] = utils.convertToRddDocument("nulls_count", nullsCount: Long)

          writers.writeRddToMongo(nullsCountRdd, column)

          val mostCommonValue: DataFrame = counters.findMostCommonValue(column, df)
          writers.writeDfToMongo("most_common_value", "append", mostCommonValue)
        }
      }
    }
  }

}
