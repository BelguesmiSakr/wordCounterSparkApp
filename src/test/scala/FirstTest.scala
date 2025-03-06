package org.sakr.countWordSparkScalaApp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import Utils.buildSparkSession

//todo: variable names / test styles / reusable code / case Class / (dataframe vs dataset vs RDD)
class FirstTest extends AnyFunSuite{
  private val spark = buildSparkSession("testApp")

  def createDataFrameFromText(text:String)(implicit spark:SparkSession):DataFrame={
    import spark.implicits._
    val words = text.split(" ").toSeq
    val testDataFrame = words.toDF("value")
    testDataFrame
  }

  test("wordCounter function should return zero when the dataframe is empty"){ // unit test for wordCounter function
    val testDataFrame = createDataFrameFromText("")(spark)
    val result = Main.countWords(testDataFrame)
    val expectedWorldCount = 0

    assert(result == expectedWorldCount,s"❌ Test failed expected $expectedWorldCount but got $result") // asserting the expected result and the result returned by wordCounter
    println("✅ Test Passed: wordCounter function returned 0 in case of empty dataFrame")
  }

  test("wordCounter function should return the number of words in the dataframe"){ // unit test for wordCounter function
    val testDataFrame = createDataFrameFromText("one tow three four five six")(spark)
    val result = Main.countWords(testDataFrame)
    val expectedWorldCount = 6

    assert(result == expectedWorldCount,s"❌ Test failed expected $expectedWorldCount but got $result") // asserting the expected result and the result returned by wordCounter
    println("✅ Test Passed: wordCounter function returned the correct word count!")
  }
}