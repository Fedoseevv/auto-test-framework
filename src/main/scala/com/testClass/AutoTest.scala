package com.testClass

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.io.Source
import com.helpfulClasses.CustomLogger

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
case class AutoTest(private val spark: SparkSession, private val settingPath: String) {
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
    logger.info("----- All tests result -----")
    for (item <- results) {
      if (item._2 == true) // Если результат теста положительный
        logger.info(s"${item._1} - *** result: 'SUCCESS' ***")
      else
        logger.error(s"${item._1} - *** result: 'FAILED' ***")
      if (item._2 == false) // Если результат теста отрицательный, изменяем флаг
        flag = true
    }
    if (flag) // В случае обнаружение FAILED-теста "выбрасываем" исключение
      throw new Exception("Not all tests were successful! *** FAILED ***")
    else
      println("All tests were successful! *** COMPLETED ***")
  }

  // Метод, реализующий тестирование количества строк, на вход получает пути до двух sql-запросов (source и target)
  def isEqualCounts(countSourcePath: String, countTargetPath: String): Boolean = {
    logger.info("isEqualCounts started...")
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
    logger.info("isEqualDF started...")
    var file = Source.fromFile(sourceSQLPath)
    val sourceResult: DataFrame = spark.sql(file.mkString) // Получаем первый датафрейм

    file = Source.fromFile(targetSQLPath)
    val targetResult: DataFrame = spark.sql(file.mkString) // Получаем второй датафрейм
    file.close()
    val rowCountSource = sourceResult.count() // Количество строк первого DF
    val rowCountTarget = targetResult.count() // Количество строк второго DF
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
      } else { // Если количество строк различает, except можно уже не делать, логгируем FAILED
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
    logger.info("isEqualSchemas started...")
    var file = Source.fromFile(sourceDDLPath)
    val sourceDDL = spark.sql(file.mkString)
    file = Source.fromFile(targetDDLPath)
    val targetDDL = spark.sql(file.mkString)
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

  // Метод, реализующий сравнение двух результата sql-запроса и содержимого файла,
  // на вход получает пути до sql-запроса и текстового файла
  def isEqualConstants(sourcePath: String, targetResPath: String): Boolean = {
    logger.info("isEqualConstants started...")
    // [1,Клиент 1,Ф,-,2020-11-01] - строка, приведенная к элементу массива
    var file = Source.fromFile(sourcePath)
    val sourceResult = spark.sql(file.mkString) // Результат sql-запроса

    file = Source.fromFile(targetResPath)
    val targetRes = file.getLines().toArray // Содержимое текстового файла записываем в массив
    file.close()

    targetRes.map(str => str.trim())
    val source = sourceResult.collect() // Сохраняем результат sql-запроса в массив

    if (source.length == targetRes.length) { // Если количество строк совпадает, то начинаем сравнивать построчно
      for (row <- source) {
        if (targetRes.indexOf(row.toString()) == -1) { // Если какой-либо строки нет, то логгируем FAILED и выходим
          logTest(sourcePath, res = false)
          return false
        }
      }
    } else { // Если количество строк не совпадает, то логгируем FAILED и выходим
      logTest(sourcePath, res = false)
      return false
    }
    logTest(sourcePath, res = true) // Если в процессе сравнение все строки были найдены, то логгируем SUCCESS
    true
  }

}
