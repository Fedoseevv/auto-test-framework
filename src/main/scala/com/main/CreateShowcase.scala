package com.main

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

// В данном файле создается большая витрина данных и записывается в csv

object CreateShowcase {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\opt\\spark-2.4.7-bin-hadoop2.7")
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Auto-test framework")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")


    val AccountSchema = new StructType()
      .add("account_num", IntegerType)
      .add("account_balance", LongType)
      .add("open_dt", StringType)
      .add("id", IntegerType)
      .add("client_type", StringType)

    val CompaniesSchema = new StructType()
      .add("company_id", IntegerType)
      .add("company_name", StringType)
      .add("birth_dt", StringType)

    val OperationSchema = new StructType()
      .add("operation_id", IntegerType)
      .add("account_num", IntegerType)
      .add("account_num_recipient", IntegerType)
      .add("operation_sum", LongType)
      .add("operation_date", StringType)

    val UsersSchema = new StructType()
      .add("user_id", IntegerType)
      .add("last_nm", StringType)
      .add("first_nm", StringType)
      .add("middle_nm", StringType)
      .add("birth_dt", StringType)

    val users = spark.read.option("header", "true")
      .option("delimiter", ";").schema(UsersSchema)
      .csv("D:\\samples\\Bank.Users")
    users.createOrReplaceTempView("Users")

    val accounts = spark.read.option("header", "true")
      .option("delimiter", ";").schema(AccountSchema)
      .csv("D:\\samples\\Bank.Accounts")
    accounts.createOrReplaceTempView("Accounts")

    val companies = spark.read.option("header", "true")
      .option("delimiter", ";").schema(CompaniesSchema)
      .csv("D:\\samples\\Bank.Companies")
    companies.createOrReplaceTempView("Companies")

    val operations = spark.read.option("header", "true")
      .option("delimiter", ";").schema(OperationSchema)
      .csv("D:\\samples\\Bank.Operations")
    operations.createOrReplaceTempView("Operations")

    val showcase = spark.sql(
      """
        |select
        |    t.id,
        |    t.account_num,
        |    sum(t.income) as income,
        |    sum(t.outcome) as outcome,
        |    t.client_type
        |from
        |    (
        |        select
        |            acc.id,
        |            acc.account_num,
        |            SUM(oper.operation_sum) as income,
        |            0 as outcome,
        |            acc.client_type
        |        from
        |            Accounts acc
        |                left join Operations oper on
        |                    acc.account_num = oper.account_num_recipient
        |        group by acc.id, acc.account_num, acc.client_type
        |        union
        |        select
        |            acc.id,
        |            acc.account_num,
        |            0 as income,
        |            SUM(oper.operation_sum) as outcome,
        |            acc.client_type
        |        from
        |            Accounts acc
        |                left join Operations oper on
        |                    acc.account_num = oper.account_num
        |        group by acc.id, acc.account_num, acc.client_type) as t
        |group by t.id, t.account_num, t.client_type
        |""".stripMargin)
    showcase.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", value = true)
      .option("delimiter", ";")
      .mode(SaveMode.Overwrite).save("D:\\showcase")
  }
}
