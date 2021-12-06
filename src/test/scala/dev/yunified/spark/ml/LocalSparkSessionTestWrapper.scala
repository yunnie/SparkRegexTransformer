package dev.yunified.spark.ml

import org.apache.spark.sql.SparkSession

trait LocalSparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Spark local test")
      .getOrCreate()
  }
}
