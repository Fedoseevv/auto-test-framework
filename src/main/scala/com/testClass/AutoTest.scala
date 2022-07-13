package com.testClass

import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

import scala.collection.mutable
import scala.io.Source
import com.helpfulClasses.CustomLogger

import java.io.{File, FileNotFoundException}
import java.util.zip.ZipInputStream
import scala.util.matching.Regex
// Кейс-класс, содержащий описание структуры DDL. Используется в тестах isEqualSchema
case class DdlRecord(col_name: String, data_type: String)

/**
 * Данный класс реализует функционал для 4 различных кейсов тестирования
 * 1) isEqualCounts - Сравнивает результаты двух sql-запросов, направленных на подсчет строк в таблице
 * 2) isEqualDF - Сравнивает результаты двух sql-запросов, формирующих датафреймы
 * 3) isEqualSchemas - Сравнивает атрибут describe заданной таблицы с входным DDL
 * 4) isEqualConstants - Сравнивает результат sql-запроса с входными данными, содержащими ожидаемый результат
 *
 */
case class AutoTest(private val spark: SparkSession) {
  // Содержит результаты пройденных тестов (название -> результат). Результатом может быть либо true, либо, в случае
  // FAILED-теста, причина, по которой тест не пройден
  private val results = mutable.Map.empty[String, Any]
  val logger = new CustomLogger // Логгер для логгирования результатов

  private def getTestName(name: String): String = name.split("\\\\|//").last
    .replaceAll(new Regex("source_|target_|/").regex, "")
    .replaceAll(new Regex("\\.\\w+").regex, "")

  // Метод, используемый для записи в results данных о пройденных тестах
  private def logTest(name: String, res: Any): Any = {
    // Из пути к файлу "достаем" название и номер теста
    results += (getTestName(name) -> res) // Сохраяем результат очередного теста
  }

  // Метод, запускающий все тесты в локальном режиме (необходимо передать путь src/main/resources)
  def start(path: String = "src/main/resources"): Unit = {
    val filesHere = new File(path).listFiles() // Получаем все файлы в заданном каталоге
    // В зависимости от названия теста, запускаем определенный метод данного класса
    for (file <- filesHere; name = file.getName if name.matches(new Regex("test_\\d+_source_\\w+.sql").regex)) {
      if (name.contains("arrays")) {
        this.isEqualDF(s"/${name}", s"/${name.replace("source", "target")}")
      } else if (name.contains("counts")) {
        this.isEqualCounts(s"/${name}", s"/${name.replace("source", "target")}")
      } else if (name.contains("ddl")) {
        this.isEqualSchemas(s"/${name}", s"/${name.replace("source", "target")}")
      } else if (name.contains("constants")) {
        this.isEqualConstants(s"/${name}", s"/${name.replace("source", "target")}")
      } else {
        this.isEqualDataframes(s"/$name", s"/${name.replace("source", "target")}")
      }
    }
    this.parseResult() // После запуска всех тестов печатаем их результаты
  }
  // Метод, для запуска тестирования в контуре
  def startTestingOnCluster(): Unit = {
    // Ниже создаем файловую систему из файла JAR(zip), а затем обходим каталоги и ищем по маске файлы тестов
    val src = getClass.getProtectionDomain.getCodeSource
    val jar = src.getLocation // Получаем путь до файла jar
    val zip = new ZipInputStream(jar.openStream()) // Создаем файловую систему из файла JAR(zip)
    val reg = new Regex("test_\\d+_source_\\w+.sql").regex // Регулярка для поиска файлов тестов

    while (true) { // Обход каталогов
      val entry = zip.getNextEntry
      if (entry == null) { // Когда файлы в каталоге закончились, печатаем результаты тестов и выходим из метода
        this.parseResult()
        return
      }

      val name = entry.getName // Получаем название очередного файла
      if (name.matches(reg)) { // Если это файл теста, то есть он удовлетворяет регулярному выражению
        if (name.contains("arrays")) {
          this.isEqualDF(s"/$name", s"/${name.replace("source", "target")}")
        } else if (name.contains("counts")) {
          this.isEqualCounts(s"/$name", s"/${name.replace("source", "target")}")
        } else if (name.contains("ddl")) {
          this.isEqualSchemas(s"/$name", s"/${name.replace("source", "target")}")
        } else if (name.contains("constants")) {
          this.isEqualConstants(s"/$name", s"/${name.replace("source", "target")}")
        } else {
          this.isEqualDataframes(s"/$name", s"/${name.replace("source", "target")}")
        }
      }
    }
  }

  // Метод, используемый для вывода результата тестирования и "выброса" исключения, если какой-либо тест(-ы) не прошел
  def parseResult(): Unit = {
    var flag = false // false - ошибок нет, true - ошибка есть
    val failedTests: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty // Массив для сборы FAILED-тестов
    logger.info("----- All tests result -----")
    results.foreach{ item => { // Идем по всем результатам и сопоставляем с образцом
      item match {
        case p if p._2.isInstanceOf[String] => // Если в поле _2 хранится строка, значит тест FAILED
          println(s"${item._1} *** 'FAILED' *** - Reason: ${item._2}")
          flag = true // потому что найден FAILED-тест
          failedTests.append(item._1) // Добавляем название FAILED-теста
        case _ => println(s"${item._1} *** 'SUCCESS' ***") // В ином случае тест пройден успешно
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
    if (flag)  // В случае обнаружения FAILED-теста "выбрасываем" исключение
      throw new Exception(s"Failed tests: ${failedTests.mkString(", ")} - *** FAILED ***")
    else
      println("All tests were successful! *** COMPLETED ***")
  }

  // Метод, реализующий сравнение количества строк, на вход получает имена двух sql-запросов (source и target)
  def isEqualCounts(countSourcePath: String, countTargetPath: String): Boolean = {
    logger.info(s"${getTestName(countSourcePath)} started...")
    try {
      // Получаем количество строк в source
      var stream = getClass.getResourceAsStream(countSourcePath)
      val sourceQuery = Source.fromInputStream(stream).mkString
      val sourceCount: Int = spark.sql(sourceQuery).collect()(0) // Получаем результат первого запроса
        .toString().replace("[", "").replace("]", "").toInt // и приводим его к INT

      // Получаем количество строк в target
      stream = getClass.getResourceAsStream(countTargetPath)
      val targetQuery = Source.fromInputStream(stream).mkString
      val targetCount: Int = spark.sql(targetQuery).collect()(0).toString() // Получаем результат второго запроса
        .replace("[", "").replace("]", "").toInt // и приводим его к INT
      stream.close()

      if (sourceCount != targetCount) { // Если результаты НЕ совпали, то логгируем FAILED-тест, указывая причину
        logTest(countSourcePath, s"!!! source-count: $sourceCount doesn't match target-count: $targetCount !!!")
        false // Выходим из метода
      } else { // Если результаты совпали, то логгируем с true
        logTest(countSourcePath, res = true)
        true // Выходим из метода
      }
    } catch {
      case ex: FileNotFoundException => // Если какой-то из файлов отсутствует в директории src/main/resources
        logTest(countSourcePath, "!!! Invalid file path !!!")
        false
      case ex: AnalysisException => // Данный тип ошибки м.б при отсутствии какой-либо таблицы БД,
        // либо при несовпадении схем двух df
        logTest(countSourcePath, ex.message.split(";")(0))
        false
      case ex: Throwable =>  // Другие ошибки
        logTest(countSourcePath, ex.getMessage)
        false
    }
  }

  // Метод, реализующий полное сравнение двух датафреймов, на вход получает имена двух sql-запросов (source и target)
  def isEqualDF(sourceSQLPath: String, targetSQLPath: String): Boolean = {
    logger.info(s"${getTestName(sourceSQLPath)} started...")
    try {
      // Считываем первый sql-запрос
      var stream = getClass.getResourceAsStream(sourceSQLPath)
      val sourceQuery = Source.fromInputStream(stream).mkString
      val sourceResult: DataFrame = spark.sql(sourceQuery) // Получаем первый датафрейм

      // Считываем второй sql-запрос
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
        } else { // Если except вернул НЕ пустой df, то логгируем FAILED-тест и указываем причину
          logTest(sourceSQLPath, s"source dataframe doesn't match with target dataframe")
          false // Выходим из метода
        }
      } else { // Если количество строк различается, except можно уже не делать, логгируем FAILED
        logTest(sourceSQLPath, s"!!! source-count: $rowCountSource doesn't match target-count $rowCountTarget !!!")
        false // Выходим из метода
      }
    } catch {
      case ex: FileNotFoundException => // Если какой-либо файл не удалось найти в папке ресурсов
        logTest(sourceSQLPath, "!!! Target file for this test not found !!!")
        false
      case ex: AnalysisException => // Данный тип ошибки м.б при отсутствии какой-либо таблицы БД,
        // либо при несовпадении схем двух df
        logTest(sourceSQLPath, ex.message.split(";")(0))
        false
      case ex: Throwable =>  // Другие ошибки
        logTest(sourceSQLPath, ex.getMessage)
        false
    }
  }

  // Метод для чтения DDL таблицы в формате csv из файла в папке resources
  private def readDdlFromResource(path: String, del: String = ";"): DataFrame = {
    val stream = getClass.getResourceAsStream(path)
    val file = Source.fromInputStream(stream).getLines().toSeq // Считываем все строки

    val rdd = spark.sparkContext.parallelize(file) // Создаем RDD
    val records = rdd.map(x => x.split(del)).map { // Преобразуем все строки к экземплярам кейс-класса DdlRecord
      case Array(name, data_type) => DdlRecord(name, data_type)
    }

    spark.createDataFrame(records) // Возвращаем датафрейм, содержащий DDL таблицы
  }

  // Метод, реализующий сравнение двух DDL, на вход sql-запрос (describe Table) и .sql-файл, содержащий DDL-таблицы в формате csv
  def isEqualSchemas(sourceDDLPath: String, targetDDLPath: String): Boolean = {
    logger.info(s"${getTestName(sourceDDLPath)} started...")
    try {
      // Считываем датафрейм и удаляем столбец с комментариями
      val sourceDDL: DataFrame = readDdlFromResource(sourceDDLPath) //.drop("comment")

      // Считываем датафрейм и удаляем столбец с комментариями
      val stream = getClass.getResourceAsStream(targetDDLPath)
      val query = Source.fromInputStream(stream).mkString
      val targetDDL = spark.sql(query).drop("comment")
      stream.close()

      // Если количество строк совпадает и except выдает пустой df, то схемы эквивалентны
      if (sourceDDL.count() - targetDDL.count() == 0 && sourceDDL.except(targetDDL).count == 0) {
        logTest(sourceDDLPath, res = true)
        true
      } else { // Иначе логгируем результат теста, указав причину FAILED
        logTest(sourceDDLPath, "!!! source DDL doesn't match with target DDL !!!")
        false
      }
    } catch { // Сюда попадем, если схемы df не совпадают => результаты они вернули тоже разные => логгируем FAILED
      case ex: FileNotFoundException =>
        logTest(sourceDDLPath, "!!! Target file for this test not found !!!")
        false
      case ex: AnalysisException => // Данный тип ошибки м.б при отсутствии какой-либо таблицы БД,
        // либо при несовпадении схем двух df
        logTest(sourceDDLPath, ex.message.split(";")(0))
        false
      case ex: Throwable => { // Другие ошибки
        logTest(sourceDDLPath, ex.getMessage)
        false
      }
    }
  }

  // Метод, реализующий сравнение результата sql-запроса с заданным файлом .sql, содержащим ожидаемый результат в формате csv
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
      // Заменяем ";" на ",", так как строки содержат разделитель ","
      target = target.map(str => str.trim.replaceAll(";", ",")) // Удаляем лишние пробелы, символы конца строк
      val source = sourceDf.collect().map(str => str.toString() // Преобразуем результат запроса source в массив строк
        .replace("[", "").replace("]", ""))

      if (source.length == target.length) { // Если количество строк совпадает, то начинаем сравнивать массивы
        for (ind <- source.indices) { // Перебираем строки в source и сравниваем с соответствующей строкой в target
          if (source(ind) != target(ind)) { // (Работает только если массивы отсортированы)
            logTest(sourcePath, s"!!! source row (${source(ind)}) isn't contained in target result !!!")
            return false
          }
        }
//        for (row <- source) { // Проходим по строкам полученного результата и ищем их в target
//          if (target.indexOf(row) == -1) { // Если какой-либо строки нет, то логгируем результат FAILED, указав строку, которой нет
//            logTest(sourcePath, s"!!! source row ($row) isn't contained in target result !!!")
//            return false
//          }
//        }
        logTest(sourcePath, res = true) // Если все строки содержатся в target, то логгируем SUCCESS-результат
        true
      } else { // Иначе логгируем FAILED-тест, указав причину о разности количества строк
        logTest(sourcePath, s"!!! source-count: ${source.length} doesn't match with target-count ${target.length} !!!")
        false
      }
    } catch {
      case ex: FileNotFoundException => // Если путь до какого-либо файла указан некорректно
        logTest(sourcePath, "!!! Target file for this test not found !!!")
        false
      case _: Throwable =>
        logTest(sourcePath, "!!! Unexpected error. Check input files and try again !!!")
        false
    }
  }

  def isEqualDataframes(sourcePath: String, targetPath: String): Boolean = {
    logger.info(s"${getTestName(sourcePath)} started...")
    try {
      // Считываем первый sql-запрос
      var stream = getClass.getResourceAsStream(sourcePath)
      val sourceQuery = Source.fromInputStream(stream).mkString
      val sourceResult: DataFrame = spark.sql(sourceQuery) // Получаем первый датафрейм

      // Считываем второй sql-запрос
      stream = getClass.getResourceAsStream(targetPath)
      val targetQuery = Source.fromInputStream(stream).mkString
      val targetResult: DataFrame = spark.sql(targetQuery) // Получаем второй датафрейм
      stream.close()

      // Если количество строк в датафреймах одинаковое и except выдает пустой df, тогда они эквивалентны
      if (sourceResult.count() == targetResult.count() && sourceResult.except(targetResult).count() == 0) {
        logTest(sourcePath, res = true)
        true // Выходим из метода
      } else {
        logTest(sourcePath, "!!! Source dataframe doesn't equal target dataframe !!!")
        false // Выходим из метода
      }
    } catch {
      case ex: FileNotFoundException => // Если какой-либо файл не удалось найти в папке ресурсов
        logTest(sourcePath, "!!! Target file for this test not found !!!")
        false

      case ex: AnalysisException => // Данный тип ошибки м.б при отсутствии какой-либо таблицы БД,
        // либо при несовпадении схем двух df
        logTest(sourcePath, ex.message.split(";")(0))
        false
      case ex: Throwable =>  // Другие ошибки
        logTest(sourcePath, ex.getMessage)
        false

    }
  }
}