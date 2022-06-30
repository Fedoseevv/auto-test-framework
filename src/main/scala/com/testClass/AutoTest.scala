package com.testClass

import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

import scala.collection.mutable
import scala.io.Source
import com.helpfulClasses.CustomLogger

import java.io.{File, FileNotFoundException}
import java.util.zip.ZipInputStream
import scala.util.matching.Regex
case class DdlRecord(col_name: String, data_type: String, comment: String)

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

  private def getTestName(name: String): String = name.split("\\\\|//").last
    .replaceAll(new Regex("source_|target_|/").regex, "")
    .replaceAll(new Regex("\\.\\w+").regex, "")

  // Метод, используемый для записи в results данных о пройденных тестах
  private def logTest(name: String, res: Any): Any = {
    // Из пути к файлу достаем название и номер теста
    results += (getTestName(name) -> res) // Сохраяем результат очередного теста
  }

  // Метод, запускающий все тесты
  def start(path: String): Unit = {

    val filesHere = new File(path).listFiles()
    for (file <- filesHere; name = file.getName if name.matches(new Regex("test_\\d+_source_\\w+.sql").regex)) {
      if (name.contains("arrays")) {
        this.isEqualDF(s"/${name}", s"/${name.replace("source", "target")}")
      } else if (name.contains("counts")) {
        this.isEqualCounts(s"/${name}", s"/${name.replace("source", "target")}")
      } else if (name.contains("ddl")) {
        this.isEqualSchemas(s"/${name}", s"/${name.replace("source", "target")}")
      } else {
        this.isEqualConstants(s"/${name}", s"/${name.replace("source", "target")}")
      }
    }

    this.parseResult()
  }

  def startTestingOnCluster(): Unit = {
    val src = getClass.getProtectionDomain.getCodeSource
    val jar = src.getLocation
    val zip = new ZipInputStream(jar.openStream())
    val reg = new Regex("test_\\d+_source_\\w+.sql").regex

    while (true) {
      val entry = zip.getNextEntry
      if (entry == null) {
        this.parseResult()
        return
      }

      val name = entry.getName
      if (name.matches(reg)) {
        if (name.contains("arrays")) {
          this.isEqualDF(s"/$name", s"/${name.replace("source", "target")}")
        } else if (name.contains("counts")) {
          this.isEqualCounts(s"/$name", s"/${name.replace("source", "target")}")
        } else if (name.contains("ddl")) {
          this.isEqualSchemas(s"/$name", s"/${name.replace("source", "target")}")
        } else {
          this.isEqualConstants(s"/$name", s"/${name.replace("source", "target")}")
        }
      }
    }
  }

  // Метод, используемый для вывода вывода результата и "выброса" исключение, если какой-либо тест не прошел
  def parseResult(): Unit = {
    var flag = false // false - ошибок нет, true - ошибка есть
    val failedTests: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty
    logger.info("----- All tests result -----")
    results.foreach{ item => {
      item match {
        case p if p._2.isInstanceOf[String] =>
          println(s"${item._1} *** 'FAILED' *** - Reason: ${item._2}")
          flag = true
          failedTests.append(item._1)
        case _ => println(s"${item._1} *** 'SUCCESS' ***")
      }
    } }
//    for (item <- results) {
//      if (item._2 == true) // Если результат теста положительный
//        logger.info(s"${item._1} - *** result: 'SUCCESS' ***")
//      else
//        logger.error(s"${item._1} - *** result: 'FAILED' ***")
//
//      if (item._2 == false) { // Если результат теста отрицательный, изменяем флаг
//        flag = true
//        failedTests.append(item._1)
//      }
//    }
    if (flag)  // В случае обнаружение FAILED-теста "выбрасываем" исключение
      throw new Exception(s"Failed tests: ${failedTests.mkString(", ")} - *** FAILED ***")
    else
      println("All tests were successful! *** COMPLETED ***")
  }

  // Метод, реализующий сравнение количества строк, на вход получает пути до двух sql-запросов (source и target)
  def isEqualCounts(countSourcePath: String, countTargetPath: String): Boolean = {
    logger.info(s"${getTestName(countSourcePath)} started...")
    try {
      var stream = getClass.getResourceAsStream(countSourcePath)
      val sourceQuery = Source.fromInputStream(stream).mkString
      val sourceCount: Int = spark.sql(sourceQuery).collect()(0) // Получаем результат первого запроса
        .toString().replace("[", "").replace("]", "").toInt // и приводим его к INT

      stream = getClass.getResourceAsStream(countTargetPath)
      val targetQuery = Source.fromInputStream(stream).mkString
      val targetCount: Int = spark.sql(targetQuery).collect()(0).toString() // Получаем результат второго запроса
        .replace("[", "").replace("]", "").toInt // и приводим его к INT
      stream.close()

      if (sourceCount != targetCount) { // Если результаты не совпали, то логгируем с false
        logTest(countSourcePath, s"!!! source-count: $sourceCount doesn't match target-count: $targetCount !!!")
        false // Выходим из метода
      } else { // Если результаты совпали, то логгируем с true
        logTest(countSourcePath, res = true)
        true // Выходим из метода
      }
    } catch {
      case ex: FileNotFoundException =>
        logTest(countSourcePath, "!!! Invalid file path !!!")
        false
      case _: Throwable =>
        logTest(countSourcePath, "!!! the schema of the result does not match the schema of the expected !!!")
        false
    }
  }

  // Метод, реализующий полное сравнение двух датафреймов, на вход получает пути до двух sql-запросов (source и target)
  def isEqualDF(sourceSQLPath: String, targetSQLPath: String): Boolean = {
    logger.info(s"${getTestName(sourceSQLPath)} started...")
    try {
      var stream = getClass.getResourceAsStream(sourceSQLPath)
      val sourceQuery = Source.fromInputStream(stream).mkString
      val sourceResult: DataFrame = spark.sql(sourceQuery) // Получаем первый датафрейм

      stream = getClass.getResourceAsStream(targetSQLPath)
      val targetQuery = Source.fromInputStream(stream).mkString
      val targetResult: DataFrame = spark.sql(targetQuery) // Получаем второй датафрейм
      stream.close()

      val rowCountSource = sourceResult.count() // Количество строк source DF
      val rowCountTarget = targetResult.count() // Количество строк target DF
      val rowCountDiff: Long = Math.abs(rowCountSource - rowCountTarget) // Разница в количестве строк между DF

      if (rowCountDiff == 0) { // Если количество строк совпадает, то выполняем except и проверяем пустой результат или нет
        if (targetResult.except(sourceResult).count == 0) { // Если пустой, значит тест прошел
          logTest(sourceSQLPath, res = true)
          true // Выходим из метода
        } else { // Если except вернул НЕ пустой df, то логгируем false
          logTest(sourceSQLPath, s"source dataframe doens't match with target dataframe")
          false // Выходим из метода
        }
      } else { // Если количество строк различается, except можно уже не делать, логгируем FAILED
        logTest(sourceSQLPath, s"!!! source-count: $rowCountSource doesn't match target-count $rowCountTarget !!!")
        false // Выходим из метода
      }
    } catch { // Сюда попадем, если схемы df не совпадают => результаты они вернули тоже разные => логгируем FAILED
      case ex: FileNotFoundException =>
        logTest(sourceSQLPath, "!!! Invalid file path !!!")
        false
      case _: Throwable =>
        logTest(sourceSQLPath, "!!! the schema of the result does not match the schema of the expected !!!")
        false
    }
  }

  // Метод для чтения DDL таблицы в формате csv из файла в папке resources
  private def readDdlFromResource(path: String, del: String = ","): DataFrame = {
    val stream = getClass.getResourceAsStream(path)
    val file = Source.fromInputStream(stream).getLines().toSeq // Считываем все строки

    val rdd = spark.sparkContext.parallelize(file) // Создаем RDD
    val records = rdd.map(x => x.split(del)).map { // Преобразуем все строки к экземплярам кейс-класса DdlRecord
      case Array(name, data_type, comment) => DdlRecord(name, data_type, comment)
    }

    spark.createDataFrame(records)
  }

  // Метод, реализующий сравнение двух DDL, на вход получает пути до двух DDL (source и target)
  def isEqualSchemas(sourceDDLPath: String, targetDDLPath: String): Boolean = {
    logger.info(s"${getTestName(sourceDDLPath)} started...")
    try {
      // Считываем датафрейм и удаляем столбец с комментариями
      val sourceDDL: DataFrame = readDdlFromResource(sourceDDLPath).drop("comment")

      // Считываем датафрейм и удаляем столбец с комментариями
      val stream = getClass.getResourceAsStream(targetDDLPath)
      val query = Source.fromInputStream(stream).mkString
      val targetDDL = spark.sql(query).drop("comment")
      stream.close()

      // Если количество строк совпадает и except выдает пустой df, то схемы эквивалентны
      if (sourceDDL.count() - targetDDL.count() == 0 && sourceDDL.except(targetDDL).count == 0) {
        logTest(sourceDDLPath, res = true)
        true
      } else {
        logTest(sourceDDLPath, "!!! source DDL doesn't match with target DDL !!!")
        false
      }
    } catch { // Сюда попадем, если схемы df не совпадают => результаты они вернули тоже разные => логгируем FAILED
      case ex: FileNotFoundException =>
        logTest(sourceDDLPath, "!!! Invalid file path !!!")
        false
      case _: Throwable => {
        logTest(sourceDDLPath, "!!! source DDL doesn't match with target DDL !!!")
        false
      }
    }
  }

  // Метод, реализующий сравнение результата sql-запроса с заданным csv-файлом
  def isEqualConstants(sourcePath: String, targetPath: String): Boolean = {
    logger.info(s"${getTestName(sourcePath)} started...")
    try {
      // Считываем sql-запрос
      var stream = getClass.getResourceAsStream(sourcePath)
      val sourceQuery = Source.fromInputStream(stream).mkString
      val sourceDf = spark.sql(sourceQuery) // Результат sql-запроса

      // Считываем ожидаемый результат и записываем в массив target
      stream = getClass.getResourceAsStream(targetPath)
      var target = Source.fromInputStream(stream).getLines().toArray
      stream.close()

      target = target.map(str => str.trim) // Удаляем лишние пробелы, символы конца строк
      val source = sourceDf.collect().map(str => str.toString()
        .replace("[", "").replace("]", ""))

      if (source.length == target.length) {
        for (row <- source) {
          if (target.indexOf(row) == -1) {
            logTest(sourcePath, s"!!! source row ($row) isn't contained in target result !!!")
            return false
          }
        }
        logTest(sourcePath, res = true)
        true
      } else {
        logTest(sourcePath, s"!!! source-count: ${source.length} doesn't match with target-count ${target.length} !!!")
        false
      }
    } catch {
      case ex: FileNotFoundException =>
        logTest(sourcePath, "!!! Invalid file path !!!")
        false
      case _: Throwable =>
        logTest(sourcePath, "!!! Unexpected error. Check input files and try again !!!")
        false
    }
  }
}
