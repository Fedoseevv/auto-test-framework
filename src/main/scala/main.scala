import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import DemoTester.Tester
//import DemoTester_v2.Tester_v2
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.codehaus.jettison.json.JSONArray

import scala.io.Source

object main {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\opt\\spark-2.4.7-bin-hadoop2.7")
    import org.apache.log4j.PropertyConfigurator
    PropertyConfigurator.configure("D:\\opt\\spark-2.4.7-bin-hadoop2.7\\conf\\log4j.properties")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Demo-test project")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val RateSchema = new StructType(Array(
      StructField("Currency", StringType),
      StructField("Rate", FloatType),
      StructField("RateDate", DateType)
    ))

    val AccountSchema = new StructType()
      .add("AccountID", IntegerType)
      .add("AccountNum", StringType)
      .add("ClientId", IntegerType)
      .add("DateOpen", DateType)

    val OperationSchema = new StructType()
      .add("AccountDB", IntegerType, nullable = false)
      .add("AccountCR", IntegerType, nullable = false)
      .add("DateOp", DateType, nullable = false)
      .add("Amount", FloatType, nullable = false)
      .add("Currency", StringType, nullable = false)
      .add("Comment", StringType, nullable = true)

    val ClientSchema = new StructType(Array(
      StructField("ClientId", IntegerType),
      StructField("ClientName", StringType),
      StructField("Type", StringType),
      StructField("Form", StringType),
      StructField("RegisterDate", DateType)
    ))

    val path = "src/main/resources/tmp"

    val rate: DataFrame = spark.read.schema(RateSchema)
      .option("header", value = true).option("delimiter", ";")
      .csv(path + "/tmp_rate.csv")

    val operation: DataFrame = spark.read.schema(OperationSchema)
      .option("header", value = true).option("delimiter", ";")
      .csv(path + "/tmp_operation.csv")

    val account: DataFrame = spark.read.schema(AccountSchema)
      .option("header", value = true).option("delimiter", ";")
      .csv(path + "/tmp_account.csv")

    val client: DataFrame = spark.read.schema(ClientSchema)
      .option("header", value = true).option("delimiter", ";")
      .csv(path + "/tmp_client.csv")

    val mask: DataFrame = spark.read.option("inferSchema", value = true).option("header", value = true).option("delimiter", ";")
      .csv(path + "/calculation_params_tech.csv")

    client.createOrReplaceTempView("client")
    account.createOrReplaceTempView("account")
    operation.createOrReplaceTempView("operation")
    rate.createOrReplaceTempView("rate")
    mask.createOrReplaceTempView("calculation_params_tech")

    val testSettingPath = "src/main/test/commands/test_2.json"
    Tester.startTesting(spark, testSettingPath)
  }
}