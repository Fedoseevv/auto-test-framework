package com.version2

import com.helpfulClasses.CustomLogger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex

/**
 * This class implements testing functionality in 4 different cases
 * 1) isEqualCounts - compares two sql queries on the returned result (number of rows must be the same).
 *    The input is the path to two sql queries
 * 2) isEqualDF - compares two dataframes, the input is the path to two sql queries
 * 3) isEqualSchemas - compares two DDL (showcase schemas), the input is the path to two sql files
 * 4) isEqualConstants - compares the query result with the expected data. The input is the path to the sql
 *    query and a text file containing the expected result
 */
case class AutoTest_v2(private val spark: SparkSession) {
  private val results = mutable.Map.empty[String, Any] // Содержит результаты пройденных тестов (название -> результат)
  val logger = new CustomLogger // Логгер для логгирования результатов

  // Метод, используемый для записи в results данных о пройденных тестах
  private def logTest(name: String, res: Boolean): Any = {
    // Из пути к файлу достаем название и номер теста
    val testName = name.split("/").last
      .replaceAll(new Regex("(source_|target_)").regex, "")
      .replaceAll(new Regex("\\.\\w+").regex, "")
    results += (testName -> res) // Сохраяем результат очередного теста
  }

  // Метод, используемый для вывода вывода результата и "выброса" исключение, если какой-либо тест не прошел
  def parseResult(): Unit = {
    var flag = false // false - ошибок нет, true - ошибка есть
    val failedTests: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty
    logger.info("----- All tests result -----")
    for (item <- results) {
      if (item._2 == true) // Если результат теста положительный
        logger.info(s"${item._1} - *** result: 'SUCCESS' ***")
      else {
        logger.error(s"${item._1} - *** result: 'FAILED' ***")
      }
      if (item._2 == false) { // Если результат теста отрицательный, изменяем флаг
        flag = true
        failedTests.append(item._1)
      }
    }
    if (flag)  // В случае обнаружение FAILED-теста "выбрасываем" исключение
      throw new Exception(s"Failed tests: ${failedTests.mkString(", ")} - *** FAILED ***")
    else
      println("All tests were successful! *** COMPLETED ***")
  }

  private def compareDF(leftDF: DataFrame, rightDF: DataFrame): Boolean = {
    try {
      if (leftDF.count() == rightDF.count() && leftDF.except(rightDF).isEmpty)
        true
      else
        false
    } catch {
      case _:Throwable => false
    }
  }

  // Метод, реализующий сравнение количества строк, на вход получает пути до двух sql-запросов (source и target)
  def isEqualCounts(countSourcePath: String, countTargetPath: String): Boolean = {
    logger.info("isEqualCounts started...")
    var file = Source.fromFile(countSourcePath)
    val sourceCount = spark.sql(file.mkString)

    file = Source.fromFile(countTargetPath)
    val targetCount: DataFrame = spark.sql(file.mkString)
    file.close()
    if (this.compareDF(sourceCount, targetCount)) { // Если результаты совпали, то логгируем с true
      logTest(countSourcePath, res = true)
      true // Выходим из метода
    } else { // Если результаты не совпали, то логгируем с false
      logTest(countSourcePath, res = false)
      false // Выходим из метода
    }
  }

  // Метод, реализующий полное сравнение двух датафреймов, на вход получает пути до двух sql-запросов (source и target)
  def isEqualDF(sourceSQLPath: String, targetSQLPath: String): Boolean = {
    logger.info("isEqualDF started...")
    var file = Source.fromFile(sourceSQLPath)
    val sourceResult: DataFrame = spark.sql(file.mkString) // Получаем первый датафрейм

    file = Source.fromFile(targetSQLPath)
    val targetResult: DataFrame = spark.sql(file.mkString) // Получаем второй датафрейм
    file.close()

    if (this.compareDF(sourceResult, targetResult)) {
      logTest(sourceSQLPath, res = true)
      true
    } else {
      logTest(sourceSQLPath, res = false)
      false
    }
  }

  // Метод, реализующий сравнение двух DDL, на вход получает пути до двух DDL (source и target)
  def isEqualSchemas(sourceDDLPath: String, targetDDLPath: String): Boolean = {
    logger.info("isEqualSchemas started...")
    val sourceDDL: DataFrame = spark.read.option("inferSchema", value = true).option("header", value = false)
      .csv(sourceDDLPath).toDF("col_name", "data_type", "comment").drop("comment")

    val file = Source.fromFile(targetDDLPath)
    val targetDDL = spark.sql(file.mkString).drop("comment")
    file.close()

    if (this.compareDF(sourceDDL, targetDDL)) {
      logTest(sourceDDLPath, res = true)
      true
    } else {
      logTest(targetDDLPath, res = false)
      false
    }
  }

  // Метод, реализующий сравнение результата sql-запроса с заданным csv-файлом
  def isEqualConstants(sourcePath: String, targetPath: String): Boolean = {
    logger.info("isEqualConstants started...")
    val file = Source.fromFile(sourcePath)
    val sourceDf = spark.sql(file.toList.mkString) // Результат sql-запроса
    file.close()

    val targetDf = spark.read.option("header", value = false).option("delimiter", ",") // Заданный csv-файл
      .csv(targetPath).toDF(sourceDf.columns: _*)

    if (this.compareDF(sourceDf, targetDf)) {
      logTest(sourcePath, res = true)
      true
    } else {
      logTest(targetPath, res = false)
      false
    }
  }
}
