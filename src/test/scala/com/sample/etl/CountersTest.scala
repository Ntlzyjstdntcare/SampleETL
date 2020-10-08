package com.sample.etl

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.DataFrame

class CountersTest extends FlatSpec with BeforeAndAfterAll with DataFrameSuiteBase {

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    super.sqlBeforeAllTestCases()
  }

  //I need to build an empty dataframe with the correct schema but don't
  //have time
  "A dataframe with no duplicates" should "produce an empty dataframe" in {
    val counters = new Counters(spark)

    val testDf: DataFrame = Seq(
      (8, "bat"),
      (64, "mouse"),
      (-27, "horse")
    ).toDF("number", "word")

    assertDataFrameEquals(counters.countDuplicates("number", testDf),
    spark.emptyDataFrame)
  }

  "An empty dataframe" should "produce an empty dataframe" in {
    val counters = new Counters(spark)

    val testDf: DataFrame = Seq(("", "")).toDF("number", "word")

    assertDataFrameEquals(counters.countDuplicates("number", testDf),
      spark.emptyDataFrame)
  }
}
