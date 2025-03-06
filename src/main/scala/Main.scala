package org.sakr.countWordSparkScalaApp

import org.apache.spark.sql.{DataFrame, SparkSession}
import Utils.buildSparkSession
import org.apache.spark.sql.functions._

object Main{
  def main (args: Array[String]): Unit ={
    val spark = buildSparkSession("wordCounter")

    val sourcePath = "data/sample2.txt"
    val textSplitToDF = spark.read // return dataFrame reader, read the txt file and creat a dataFrame
      .option("lineSep", " ") // optional, to use a line seperator in our case " "
      .text(sourcePath)

    textSplitToDF.show()
    countWords(textSplitToDF)
    val a=countDuplicatedWords(textSplitToDF)
    println(a)
  }
//todo persist the result => dataframe (word count / duplicated word) 
  def countWords(dataFrame: DataFrame): Long = {
    var wordCount = dataFrame.count() // count the number of rows in the dataFrame
    if (wordCount==1){
      val firstRow = dataFrame.head() // Get the first row
      if (firstRow.isNullAt(0) || firstRow.getString(0).trim.isEmpty) {
        wordCount = 0
      }
    }
    println(s"result ==> $wordCount") // print the result to console
    wordCount
  }

  def countDuplicatedWords(dataFrame: DataFrame):Long={
    val wordPattern = "^[a-zA-Z]+$"
    dataFrame
      .filter(col("value").rlike(wordPattern))
      .groupBy("value")
      .count()
      .filter(col("count")>1)
      .agg(sum("count"))
      .collect()(0)(0)
      .asInstanceOf[Long]
  }
}
