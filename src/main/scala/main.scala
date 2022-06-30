import org.apache.spark.sql.{DataFrame, SparkSession}
import com.testClass.AutoTest
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

object main {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\opt\\spark-2.4.7-bin-hadoop2.7")
    import org.apache.log4j.PropertyConfigurator
    PropertyConfigurator.configure("D:\\opt\\spark-2.4.7-bin-hadoop2.7\\conf\\log4j.properties")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Auto-test framework")
//      .enableHiveSupport()
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

    val path = "src/main/test"

    val accounts: DataFrame = spark.read.schema(AccountSchema)
      .option("header", value = true).option("delimiter", ",")
      .csv(path + "/Bank.Accounts_sample.csv")
    accounts.createOrReplaceTempView("Accounts")

    val companies: DataFrame = spark.read.schema(CompaniesSchema)
      .option("header", value = true).option("delimiter", ",")
      .csv(path + "/Bank.Companies_sample.csv")
    companies.createOrReplaceTempView("Companies")

    val operations: DataFrame = spark.read.schema(OperationSchema)
      .option("header", value = true).option("delimiter", ",")
      .csv(path + "/Bank.Operations_sample.csv")
    operations.createOrReplaceTempView("Operations")

    val users: DataFrame = spark.read.schema(UsersSchema)
      .option("header", value = true).option("delimiter", ",")
      .csv(path + "/Bank.Users_sample.csv")
    users.createOrReplaceTempView("Users")

    spark.sql(
      """
        |select
        |           t.id,
        |	   t.account_num,
        |	   sum(t.income) as income,
        |	   sum(t.outcome) as outcome,
        |	   t.client_type
        |from
        |(
        |select
        |           acc.id,
        |	   acc.account_num,
        |	   SUM(oper.operation_sum) as income,
        |	   0 as outcome,
        |	   acc.client_type
        |from
        |           Accounts acc
        |left join Operations oper on
        |           acc.account_num = oper.account_num_recipient
        |group by acc.id, acc.account_num, acc.client_type
        |union
        |select
        |           acc.id,
        |	   acc.account_num,
        |	   0 as income,
        |	   SUM(oper.operation_sum) as outcome,
        |	   acc.client_type
        |from
        |           Accounts acc
        |left join Operations oper on
        |           acc.account_num = oper.account_num
        |group by acc.id, acc.account_num, acc.client_type) as t
        |group by t.id, t.account_num, t.client_type
        |""".stripMargin).createOrReplaceTempView("dm_sum_operations")

    AutoTest(spark).start(path)
  }
}