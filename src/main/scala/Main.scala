package org.sakr.countWordSparkScalaApp

import org.apache.spark.sql.{DataFrame, SparkSession}

object Main{
  def main (args: Array[String]): Unit ={
    val spark = SparkSession.builder() // creating spark session the enty point to spark app
      .appName("wordCounter")
      .master("local[*]")
      .getOrCreate()

    val sourcePath = "data/sample2.txt"
    val df1 = spark.read // return dataFrame reader, read the txt file and creat a dataFrame
      .option("lineSep", " ") // optional, to use a line seperator in our case " "
      .text(sourcePath)
    df1.show()

    wordCounter(df1)
  }

  def wordCounter(df1: DataFrame): Long = {
    val wordCount = df1.count() // count the number of rows in the dataFrame
    println(s"result ==> $wordCount") // print the result to console
    return wordCount
  }
}
