package com.main

import com.testClass.AutoTest
import org.apache.spark.sql.SparkSession

object main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("yarn")
      .appName("Auto-test framework")
      .enableHiveSupport()
      .config("hive.metastore.uris", "thrift://dn01:9083")
      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "8g")
      .config("spark.executor.instances", "2")
      .config("spark.executor.cores", "2")
      .config("spark.submit.deployMode", "cluster")
      .config("spark.sql.shuffle.partitions", "20")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    AutoTest(spark).startTestingOnCluster()
  }
}
