package com.sample.etl
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bson.Document

class Utils(sc: SparkContext) {

  def convertToRddDocument[T <: AnyVal](key: String, obj: T): RDD[Document] = {
      val doc = new Document()
      doc.put(key, obj)

      sc.parallelize(Seq(doc))
  }
}
