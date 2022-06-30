package com.testClass

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.io.Source
import com.helpfulClasses.CustomLogger

import java.io.File
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
case class AutoTest(private val spark: SparkSession) {
  private val results = mutable.Map.empty[String, Any] // Содержит результаты пройденных тестов (название -> результат)
  val logger = new CustomLogger // Логгер для логгирования результатов

  private def getTestName(name: String): String = name.split("\\\\").last
                                          .replaceAll(new Regex("(source_|target_)").regex, "")
                                          .replaceAll(new Regex("\\.\\w+").regex, "")

  // Метод, используемый для записи в results данных о пройденных тестах
  private def logTest(name: String, res: Boolean): Any = {
    // Из пути к файлу достаем название и номер теста
    results += (getTestName(name) -> res) // Сохраяем результат очередного теста
  }

  // Метод, запускающий все тесты
  def start(path: String): Unit = {
    val filesHere = new File(path).listFiles()
    for (file <- filesHere; name = file.getName if name.matches(new Regex("test_\\d+_source_\\w+.sql").regex)) {
      if (name.contains("arrays")) {
        this.isEqualDF(file.getPath, file.getPath.replace("source", "target"))
      } else if (name.contains("counts")) {
        this.isEqualCounts(file.getPath, file.getPath.replace("source", "target"))
      } else if (name.contains("ddl")) {
        this.isEqualSchemas(file.getPath, file.getPath.replace("source", "target"))
      } else {
        this.isEqualConstants(file.getPath, file.getPath.replace("source", "target"))
      }
    }

    this.parseResult()
  }

  // Метод, используемый для вывода вывода результата и "выброса" исключение, если какой-либо тест не прошел
  def parseResult(): Unit = {
    var flag = false // false - ошибок нет, true - ошибка есть
    val failedTests: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty
    logger.info("----- All tests result -----")
    for (item <- results) {
      if (item._2 == true) // Если результат теста положительный
        logger.info(s"${item._1} - *** result: 'SUCCESS' ***")
      else
        logger.error(s"${item._1} - *** result: 'FAILED' ***")

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

  // Метод, реализующий сравнение количества строк, на вход получает пути до двух sql-запросов (source и target)
  def isEqualCounts(countSourcePath: String, countTargetPath: String): Boolean = {
    logger.info(s"${getTestName(countSourcePath)} started...")
    var file = Source.fromFile(countSourcePath)
    val sourceCount = spark.sql(file.mkString).collect()(0) // Получаем результат первого запроса
      .toString().replace("[", "").replace("]", "").toInt // и приводим его к INT
    file.close()

    file = Source.fromFile(countTargetPath)
    val targetCount: Int = spark.sql(file.mkString).collect()(0).toString() // Получаем результат второго запроса
      .replace("[", "").replace("]", "").toInt // и приводим его к INT
    file.close()
    if (sourceCount != targetCount) { // Если результаты не совпали, то логгируем с false
      logTest(countSourcePath, res = false)
      false // Выходим из метода
    } else { // Если результаты совпали, то логгируем с true
      logTest(countSourcePath, res = true)
      true // Выходим из метода
    }
  }

  // Метод, реализующий полное сравнение двух датафреймов, на вход получает пути до двух sql-запросов (source и target)
  def isEqualDF(sourceSQLPath: String, targetSQLPath: String): Boolean = {
    logger.info(s"${getTestName(sourceSQLPath)} started...")
    var file = Source.fromFile(sourceSQLPath)
    val sourceResult: DataFrame = spark.sql(file.mkString) // Получаем первый датафрейм

    file = Source.fromFile(targetSQLPath)
    val targetResult: DataFrame = spark.sql(file.mkString) // Получаем второй датафрейм
    file.close()
    val rowCountSource = sourceResult.count() // Количество строк source DF
    val rowCountTarget = targetResult.count() // Количество строк target DF
    val rowCountDiff: Long = Math.abs(rowCountSource - rowCountTarget) // Разница в количестве строк между DF
    try {
      if (rowCountDiff == 0) { // Если количество строк совпадает, то выполняем except и проверяем пустой результат или нет
        if (!targetResult.except(sourceResult).isEmpty) { // Если НЕ пустой, значит тест не прошел
          logTest(sourceSQLPath, res = false)
          false // Выходим из метода
        } else { // Если except вернул пустой df, то логгируем true
          logTest(sourceSQLPath, res = true)
          true // Выходим из метода
        }
      } else { // Если количество строк различается, except можно уже не делать, логгируем FAILED
        logTest(sourceSQLPath, res = false)
        false // Выходим из метода
      }
    } catch { // Сюда попадем, если схемы df не совпадают => результаты они вернули тоже разные => логгируем FAILED
      case _: Throwable => {
        logTest(sourceSQLPath, res = false)
        false
      }
    }
  }

  // Метод, реализующий сравнение двух DDL, на вход получает пути до двух DDL (source и target)
  def isEqualSchemas(sourceDDLPath: String, targetDDLPath: String): Boolean = {
    logger.info(s"${getTestName(sourceDDLPath)} started...")
    val sourceDDL: DataFrame = spark.read.option("inferSchema", value = true)
      .option("header", value = false).option("delimiter", ",")
      .csv(sourceDDLPath).toDF("col_name", "data_type", "comment").drop("comment")

    val file = Source.fromFile(targetDDLPath)
    val targetDDL = spark.sql(file.mkString).drop("comment")
    file.close()

    try {
      // Если количество строк совпадает и except выдает пустой df, то схемы эквивалентны
      if (sourceDDL.count() - targetDDL.count() == 0 && sourceDDL.except(targetDDL).isEmpty) {
        logTest(sourceDDLPath, res = true)
        true
      } else {
        logTest(sourceDDLPath, res = false)
        false
      }
    } catch { // Сюда попадем, если схемы df не совпадают => результаты они вернули тоже разные => логгируем FAILED
      case _: Throwable => {
        logTest(sourceDDLPath, res = false)
        false
      }
    }
  }

  // Метод, реализующий сравнение результата sql-запроса с заданным csv-файлом
  def isEqualConstants(sourcePath: String, targetPath: String): Boolean = {
    logger.info(s"${getTestName(sourcePath)} started...")
    val file = Source.fromFile(sourcePath)
    val sourceDf = spark.sql(file.toList.mkString) // Результат sql-запроса
    file.close()

    val targetDf = spark.read.option("header", value = false).option("delimiter", ",") // Заданный csv-файл
      .csv(targetPath).toDF(sourceDf.columns: _*)

    try {
        if (sourceDf.count() == targetDf.count() && sourceDf.except(targetDf).isEmpty) {
          logTest(sourcePath, res = true)
          true
        } else {
          logTest(sourcePath, res = false)
          false
        }
      } catch {
      case _: Throwable => {
        logTest(sourcePath, res = false)
        false
      }
    }

  }
}
