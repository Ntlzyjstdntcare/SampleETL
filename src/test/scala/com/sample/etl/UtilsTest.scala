package com.sample.etl

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.bson.Document
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfterAll

class UtilsTest extends FlatSpec with SharedSparkContext with BeforeAndAfterAll with RDDComparisons {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  "Long" should "generate appropriate RDD" in {

    val utils = new Utils(sc)

    val testKey: String = "a_key"
    val testLong: Long = 1000L

    val doc = new Document()
    doc.put(testKey, testLong)

    val expectedRDD: RDD[Document] = sc.parallelize(Seq(doc))
    val resultRDD: RDD[Document] = utils.convertToRddDocument(testKey, testLong)

    assertRDDEquals(expectedRDD, resultRDD)
  }

}
