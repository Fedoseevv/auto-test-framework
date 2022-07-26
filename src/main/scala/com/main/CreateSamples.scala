package com.main
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

// В данном файле создаются большие сэмплы для тестирования

object CreateSamples {
  case class Users(user_id: String, last_nm: String, first_nm: String, middle_nm: String, birth_dt: String)
  case class Accounts(account_num: String, account_balance: String, open_dt: String, id: String, client_type: String)
  case class Companies(company_id: String, company_name: String, birth_dt: String)
  case class Operations(operation_id: String, account_num: String, account_num_recipient: String, operation_sum: String, operation_date: String)

  def getRandomBetween(start: Int, end: Int): Int = {
    var num = Random.nextInt(end)
    while (num < start) {
      num = Random.nextInt(end)
    }
    num
  }

  def getRandomDate(minYear: Int, maxYear: Int): String =
    s"${val tmp = getRandomBetween(1, 31); if (tmp.toString.length == 1) "0" + tmp.toString else tmp}" +
      s".${val tmp = getRandomBetween(1, 12); if (tmp.toString.length == 1) "0" + tmp.toString else tmp}" +
      s".${getRandomBetween(minYear, maxYear)}"


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\opt\\spark-2.4.7-bin-hadoop2.7")
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Auto-test framework")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val names = Array("Григорий", "Лев", "Андрей", "Роман", "Арсений", "Степан",
      "Владислав", "Никита", "Глеб", "Марк", "Давид", "Ярослав", "Евгений",
      "Матвей", "Фёдор", "Николай", "Алексей", "Андрей", "Артемий", "Виктор",
      "Никита", "Даниил", "Денис", "Егор", "Игорь", "Лев", "Леонид", "Павел",
      "Петр", "Роман", "Руслан", "Сергей", "Семён", "Тимофей", "Ирина",
      "Степан", "Владимир", "Тимофей", "Ярослав", "Павел", "Егор",
      "Сергей", "Владислав", "Федор", "Константин", "Максим", "Артём", "Никита",
      "Юрий", "Платон", "Денис", "Ярослав", "Мирон", "Василий", "Лев",
      "Степан", "Евгений", "Савелий", "Давид", "Григорий", "Тимур",
      "Кирилл", "Виктор", "Фёдор", "Богдан", "Константин", "Адам", "Леонид",
      "Роман", "Павел", "Артемий", "Петр", "Алексей", "Мирон", "Владимир",
      "Николай", "Руслан", "Алексей", "Юрий", "Ярослав", "Семен", "Евгений",
      "Олег", "Артур", "Петр", "Степан", "Илья", "Вячеслав", "Сергей", "Василий")
    val mNames = Array("Иванович", "Петрович", "Сергеевич", "Григорьевич", "Александрович", "Дмитриевич")
    val lNames = Array("Иванов", "Александров", "Синицын", "Чепчугов", "Архипелагов", "Даль", "Димченко", "Костромин")

    val usersData = ArrayBuffer.empty[String]
    for (counter <- 1 to 1000000) {
      val curName = names(getRandomBetween(0, names.length))
      val curMName = mNames(getRandomBetween(0, mNames.length))
      val curLName = lNames(getRandomBetween(0, lNames.length))

      usersData.append(s"$counter;$curLName;$curName;$curMName;${getRandomDate(1950, 2000)}")
    }
    val rddUsers = spark.sparkContext.parallelize(usersData)
    val recordsUsers = rddUsers.map(x => x.split(";")).map {
      case Array(user_id, last_nm, first_nm, middle_nm, birth_dt) => Users(user_id, last_nm, first_nm, middle_nm, birth_dt)
    }

    val usersSamples = spark.createDataFrame(recordsUsers)

    usersSamples.coalesce(1)
      .write.format("csv").option("header", "true")
      .option("delimiter", ";")
      .save("D:\\samples\\csv\\users.csv")

    val accountsData: ArrayBuffer[String] = ArrayBuffer.empty[String]
    var id = 1 // id пользователя (от 1 до 1000)
    var accType = "Ф"
    for (counter <- 1 to 2000000) { // Каждому челу по два счета, ЮР и ФИЗ
      val balance = getRandomBetween(1000, 100000) // Баланс
      val date = getRandomDate(2015, 2022) // Дата регистрации

      accountsData.append(s"$counter;$balance;$date;$id;$accType")
      accType = if (accType == "Ф") "Ю" else "Ф" // Меняем тип аккаунта
      id += (if (counter % 2 == 0) 1 else 0)
    }

    val rddAccounts = spark.sparkContext.parallelize(accountsData)
    val recordsAccounts = rddAccounts.map(x => x.split(";")).map {
      case Array(account_num, account_balance, open_dt, id, client_type) => Accounts(account_num, account_balance, open_dt, id, client_type)
    }

    val accountsSample = spark.createDataFrame(recordsAccounts)

    accountsSample.coalesce(1)
      .write.format("csv").option("header", "true")
      .option("delimiter", ";")
      .save("D:\\samples\\csv\\accounts.csv")

    val companiesData: ArrayBuffer[String] = ArrayBuffer.empty[String]
    for (counter <- 1 to 1000000) { // Каждому челу по два счета, ЮР и ФИЗ
      val name = "Companies" + counter.toString // Название компании
      val date = getRandomDate(2015, 2022) // Дата регистрации

      companiesData.append(s"$counter;$name;$date")
    }

    val rddComp = spark.sparkContext.parallelize(companiesData)
    val recordsComp = rddComp.map(x => x.split(";")).map {
      case Array(id, name, date) => Companies(id, name, date)
    }

    val companiesSample = spark.createDataFrame(recordsComp)

    companiesSample.coalesce(1)
      .write.format("csv").option("header", "true")
      .option("delimiter", ";")
      .save("D:\\samples\\csv\\companies.csv")


    val operationsData: ArrayBuffer[String] = ArrayBuffer.empty[String]
    for (counter <- 1 to 10000000) { // Каждому челу по два счета, ЮР и ФИЗ
      val balance = getRandomBetween(1000, 100000) // Сумма перевода
      val date = getRandomDate(2015, 2022) // Дата перевода
      val from = getRandomBetween(1, 2000000) // Отправитель
      val to = getRandomBetween(1, 2000000) // Получатель

      operationsData.append(s"$counter;$from;$to;$balance;$date")
    }

    val rddOperations = spark.sparkContext.parallelize(operationsData)
    val recordsOp = rddOperations.map(x => x.split(";")).map {
      case Array(operation_id, account_num, account_num_recipient, operation_sum, operation_date) => Operations(operation_id, account_num, account_num_recipient, operation_sum, operation_date)
    }

    val operationsSample = spark.createDataFrame(recordsOp)

    operationsSample.coalesce(1)
      .write.format("csv").option("header", "true")
      .option("delimiter", ";")
      .save("D:\\samples\\csv\\operations.csv")
  }
}
