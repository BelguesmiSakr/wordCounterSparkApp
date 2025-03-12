package org.sakr.countWordSparkScalaApp

import Main.{countDuplicatedWords, countWords, readAnalyseFile}
import Utils.buildSparkSession

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

case class Data  (value:String)
case class ResultData (Total_Words:Int,Duplicated_Words:Int)

class FirstTest extends AnyFunSuite{
  private val spark = buildSparkSession("funSuiteStyleTestApp")

  test("countWords function should return zero when the dataframe is empty"){ // unit test for wordCounter function
    val data = Seq("").map(Data)
    val testDataFrame = spark.createDataFrame(data)
    val result = countWords(testDataFrame)
    val expectedWorldCount = 0

    assert(result == expectedWorldCount,s"❌ Test failed expected $expectedWorldCount but got $result") // asserting the expected result and the result returned by wordCounter
    println("✅ Test Passed: wordCounter function returned 0 in case of empty dataFrame")
  }

  test("countWords function should return the number of words in the dataframe"){ // unit test for wordCounter function
    val data = Seq("one","tow","three","four","five","six").map(Data)
    val testDataFrame = spark.createDataFrame(data)
    val result = countWords(testDataFrame)
    val expectedWorldCount = 6

    assert(result == expectedWorldCount,s"❌ Test failed expected $expectedWorldCount but got $result") // asserting the expected result and the result returned by wordCounter
    println("✅ Test Passed: wordCounter function returned the correct word count!")
  }
}

class TestSpecStyle extends AnyFunSpec with Matchers{
  private val spark = buildSparkSession("specStyleTestApp")

  describe("countDuplicatedWords"){
    it("should return the number of duplicated words in the dataframe without considering seperated symbols as a word"){
      val data = Seq("one","tow","three","four","five","six","one","tow","three","four").map(Data)
      val testDataFrame = spark.createDataFrame(data)
      testDataFrame.show()
      val result = countDuplicatedWords(testDataFrame)
      val expectedDuplicatedWorldsCount = 4

      assert(result == expectedDuplicatedWorldsCount,s"❌ Test failed expected $expectedDuplicatedWorldsCount but got $result") // asserting the expected result and the result returned by wordCounter
      println("✅ Test Passed: countDuplicatedWords function returned the correct duplicated words count!")
    }
  }
}

class TestFlatStyle extends AnyFlatSpec with Matchers{
  implicit val spark: SparkSession = buildSparkSession("flatStyleTestApp")
  import spark.implicits._

  "readAnalyseFile" should "read text file count total words and duplicated words and persist the result dataframe in csv file" in{
    readAnalyseFile("data/sample2.txt")
    val result = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/result.csv")
      .as[ResultData]
    val schemaReturned = result.schema

    val resultExpected = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("data/mockData.csv")
      .as[ResultData]
    val schemaExpected = resultExpected.schema

    schemaExpected.printTreeString()
    schemaReturned.printTreeString()

    schemaReturned should contain theSameElementsAs(schemaExpected)
    result.collect() should contain theSameElementsAs(resultExpected.collect())
  }
}