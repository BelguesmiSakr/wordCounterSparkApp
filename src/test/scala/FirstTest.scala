package org.sakr.countWordSparkScalaApp

import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class FirstTest extends AnyFunSuite{
  private val spark = SparkSession.builder() // create a spark session the test will be run as a seperated spark app
    .appName("FirstTest")
    .master("local[*]")
    .getOrCreate()

  private val schema = StructType(Seq(StructField("value",StringType,nullable = true))) // Schema to dataframe used for test
  val testRows: Seq[Row] = Seq( // sequence of rows used to creat dataframe
    Row("test"),
    Row("for"),
    Row("word"),
    Row("counter"),
    Row("using"),
    Row("scalaTest"),
  )
  implicit val encoder: Encoder[Row] = Encoders.row(schema)
  val testDf: Dataset[Row] = spark.createDataset(testRows) // creat a dataframe that contains 6 words

  test("wordCounter function should return the number of words in the dataframe"){ // unit test for wordCounter function
    val result = Main.wordCounter(testDf) // 6 words
    assert(result == 6,s"❌ Test failed expected 6 but got $result") // asserting the expected result and the result returned by wordCounter
    println("✅ Test Passed: wordCounter function returned the correct word count!")
  }
}