package org.sakr.countWordSparkScalaApp

import org.apache.spark.sql.SparkSession

object Utils {
  //function used to create a spark session and return it to be use cross app
  def buildSparkSession(appName: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
    spark
  }
}

