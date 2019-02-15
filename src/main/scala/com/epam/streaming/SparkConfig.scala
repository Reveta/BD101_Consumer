package com.epam.streaming

import org.apache.spark.sql.SparkSession

object SparkConfig {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .getOrCreate()
}
