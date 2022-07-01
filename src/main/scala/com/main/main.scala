package com.main

import com.testClass.AutoTest
import org.apache.spark.sql.SparkSession

object main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Auto-test framework")
      .enableHiveSupport()
      .config("hive.metastore.uris", "thrift://dn01:9083")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    AutoTest(spark).startTestingOnCluster()
  }
}
