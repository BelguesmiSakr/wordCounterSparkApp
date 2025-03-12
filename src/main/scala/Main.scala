package org.sakr.countWordSparkScalaApp

import org.apache.spark.sql.{DataFrame, SparkSession}
import Utils.buildSparkSession
import org.apache.spark.sql.functions._

case class Data  (value:String)

object Main{
  def readAnalyseFile (path:String): DataFrame = {
    val spark = buildSparkSession("wordCounter")

    val fileContent = spark.read.text(path).first().getString(0)
      .trim.split("\\s+").toSeq.map(Data)
    val textSplitDF = spark.createDataFrame(fileContent)

    val totalWordsCount = countWords(textSplitDF)
    val duplicatedWordsCount =countDuplicatedWords(textSplitDF)

    val result = spark.createDataFrame(Seq((totalWordsCount,duplicatedWordsCount)))
      .toDF("Total_Words","Duplicated_Words")

    result
      .write
      .option("header","true")
      .format("csv")
      .mode("overwrite")
      .save("data/result.csv")

    val test = spark.read
      .option("header", "true")
      .csv("data/result.csv")

    result
  }

  def countWords(dataFrame: DataFrame): Long = {
    val firstRow = dataFrame.head() // Get the first row
    val count = dataFrame.count()
    val wordCount = if (count == 1 & (firstRow.isNullAt(0) || firstRow.getString(0).trim.isEmpty)) 0 else count // count the number of rows in the dataFrame
    wordCount
  }

  def countDuplicatedWords(dataFrame: DataFrame):Long={
    val wordPattern = "^[a-zA-Z]+$"
    dataFrame
      .filter(col("value").rlike(wordPattern))
      .groupBy("value")
      .count()
      .filter(col("count")>1)
      .count()
  }
}

