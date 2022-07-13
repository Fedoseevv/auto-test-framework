package com.main

import com.testClass.AutoTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

import scala.io.Source

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Auto-test framework")
//      .enableHiveSupport()
//      .config("hive.metastore.uris", "thrift://dn01:9083")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val UsersSchema = new StructType(Array(
      StructField("user_id", IntegerType),
      StructField("last_nm", StringType),
      StructField("first_nm", StringType),
      StructField("middle_nm", StringType),
      StructField("birth_dt", StringType)
    ))

    val OperationsSchema = new StructType(Array(
      StructField("operation_id", IntegerType),
      StructField("account_num", IntegerType),
      StructField("account_num_recipient", IntegerType),
      StructField("operation_sum", LongType),
      StructField("operation_date", StringType)
    ))

    val AccountsSchema = new StructType(Array(
      StructField("account_num", IntegerType),
      StructField("account_balance", LongType),
      StructField("open_dt", StringType),
      StructField("id", IntegerType),
      StructField("client_type", StringType)
    ))

    val CompaniesSchema = new StructType(Array(
      StructField("company_id", IntegerType),
      StructField("company_name", StringType),
      StructField("birth_dt", StringType)
    ))

    val users = spark.read.option("header", value = true).option("delimiter", ";").schema(UsersSchema)
      .csv(args(0) + "/Users.csv")
    users.createOrReplaceTempView("users")
    users.show()

    val operations = spark.read.option("header", value = true).option("delimiter", ";").schema(OperationsSchema)
      .csv(args(0) + "/Operations.csv")
    operations.createOrReplaceTempView("operations")
    operations.show()

    val accounts = spark.read.option("header", value = true).option("delimiter", ";").schema(AccountsSchema)
      .csv(args(0) + "/Accounts.csv")
    accounts.createOrReplaceTempView("accounts")
    accounts.show()

    val companies = spark.read.option("header", value = true).option("delimiter", ";").schema(CompaniesSchema)
      .csv(args(0) + "/Companies.csv")
    companies.createOrReplaceTempView("companies")
    companies.show()

    val source = getClass.getResourceAsStream("/showcases/dm_sum_operations.sql")
    val query = Source.fromInputStream(source).mkString

    val dm_sum_operations = spark.sql(query)
    dm_sum_operations.show()
    dm_sum_operations.createOrReplaceTempView("dm_sum_operations")

    AutoTest(spark).startTestingOnCluster()
  }
}