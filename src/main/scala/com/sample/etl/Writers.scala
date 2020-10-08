package com.sample.etl

import com.mongodb.spark.{MongoSpark, toDocumentRDDFunctions}
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

import scala.util.Try

class Writers(spark: SparkSession) {

  def writeDfToMongo(collection: String, saveMode: String,
                     df: DataFrame, database: String = "ais"): Try[Unit] = {

    Try {
      MongoSpark.save(df.write
        .option("database", database)
        .option("collection", collection)
        .mode(saveMode)
      )
    }

  }

  //Should get params from conf
  def writeRddToMongo(rdd: RDD[Document], collection: String,
                      db: String = "ais",
                      uri: String = "mongodb://127.0.0.1/"): Try[Unit] = {

    Try{
      rdd.saveToMongoDB(WriteConfig(Map("uri" -> s"$uri$db.$collection")))
    }
  }

}
