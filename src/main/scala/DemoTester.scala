import DemoTester.Tester.testLogger
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

import scala.io.Source
import scala.collection.mutable
import com.helpfulClasses.{CountException, EqualDfException, EqualQueryResultException, EqualSchemasException}
import org.apache.log4j.LogManager
import org.codehaus.jettison.json.JSONArray

object DemoTester {
  object Tester {
    private val testLogger = LogManager.getLogger("tskLogger")
    def startTesting(spark: SparkSession, settingPath: String): Unit = {
      val tester = Tester(spark, settingPath)
      val file = Source.fromFile(settingPath)
      val allTests: JSONArray = new JSONArray(file.mkString)
      file.close()
      testLogger.warn("*** !!!START complex testing!!! ***")

      for (i <- 0 until  allTests.length()) {
        val currentTest = allTests.getJSONObject(i) // Рассматриваем i-ый тест
        currentTest.get("type") match {
          case "counts" => tester.countDemoTester(currentTest.get("source_path").toString,
            currentTest.get("target_path").toString)
          case "dataframes" => tester.equalDFTester(currentTest.get("source_path").toString,
            currentTest.get("target_path").toString)
          case "schema" => tester.equalSchemaTester(currentTest.get("source_path").toString,
            currentTest.get("target_showcase").toString)
          case "constants" => tester.currentConstantsTester(currentTest.get("source_path").toString,
            currentTest.get("constants_path").toString)
          case _ => throw new Exception("Invalid test type. Check your test file!!! *** FAILED ***")
        }
      }
      testLogger.warn("*** !!!END complex testing!!! ***")
      testLogger.warn("---------------------------------------------------------------------------------")
    }
  }

  case class Tester(private val spark: SparkSession, private val settingPath: String) {

    def countDemoTester(countSourcePath: String, countTargetPath: String): Unit = {
      testLogger.warn("*** START CountDemoTester... ***")
      var file = Source.fromFile(countSourcePath)
      val sourceCount = spark.sql(file.mkString).count()
      file.close()

      file = Source.fromFile(countTargetPath)
      val targetCount  = spark.sql(file.mkString).count()
      file.close()
      if (sourceCount != targetCount) {
        testLogger.warn(s"!!! EXCEPTION !!! 'Source result' ($sourceCount rows) did not equal " +
                                    s"'Target result' ($targetCount rows)! *** FAILED *** ")
        throw CountException(s"'Source result' ($sourceCount rows) did not equal " +
                             s"'Target result' ($targetCount rows)! *** FAILED *** ")
      }
      testLogger.warn(s"!!! COMPLETE !!! 'Source result' ($sourceCount rows) equal " +
                      s"'Target result' ($targetCount rows)! *** SUCCESS *** ")
      testLogger.warn("*** END CountDemoTester... ***")
    }

    def equalDFTester(sourceSQLPath: String, targetSQLPath: String): Unit = {
      testLogger.warn("*** START EqualDFTester... ***")
      var file = Source.fromFile(sourceSQLPath)
      val sourceResult: DataFrame = spark.sql(file.mkString)
      file.close()

      file = Source.fromFile(targetSQLPath)
      val targetResult: DataFrame = spark.sql(file.mkString)
      file.close()
      val rowCountSource = sourceResult.count()
      val rowCountTarget = targetResult.count()
      val rowCountDiff: Long = Math.abs(rowCountSource - rowCountTarget)
      try {
        if (rowCountDiff == 0) {
          val difference = targetResult.except(sourceResult).count
          if (difference != 0) { //
            testLogger.warn(s"!!! EXCEPTION !!! 'Source DF' did not equal 'Target DF' " +
              s"--- row count difference: ${rowCountDiff}. *** FAILED ***")
            throw EqualDfException()
          }
        }
      } catch {
        case ex: EqualDfException => throw EqualDfException(s"'Source DF' did not equal 'Target DF' --- row count difference: ${rowCountDiff}. *** FAILED ***")
        case ex: AnalysisException => throw EqualDfException(s"'Source DF schema' did not equal 'Target DF schema'! *** FAILED ***")
        case _ => throw new Exception("Другой вид ошибки! *** FAILED ***")
      }
      testLogger.warn(s"!!! COMPLETE !!! 'Source DF' equal 'Target DF' " +
        s"--- row count difference: ${rowCountDiff}. *** SUCCESS ***")
      testLogger.warn("*** END EqualDFTester... ***")
    }

    def equalSchemaTester(sourceDDLPath: String, targetShowcaseName: String): Unit = {
      testLogger.warn("*** START EqualSchemaTester... ***")
      val showcaseDescribe = spark.sql(s"DESCRIBE $targetShowcaseName")
          .drop("Comment")

      val file = Source.fromFile(sourceDDLPath)
      val sourceDDL = file.mkString.toLowerCase
      file.close()

      val mapDDL = mutable.Map[String, String]()
      sourceDDL.split(",").foreach(item => {
          val tmp = item.split("` ")
          mapDDL += (tmp(0).replace("`", "") -> tmp(1).replaceAll(" ", ""))
      })


      if (mapDDL.size == showcaseDescribe.count()) {
        for(row <- showcaseDescribe) {
          val tmp = row.toString().replace("[", "").replace("]", "").split(",")
          if (mapDDL(tmp(0).toLowerCase) != tmp(1).toLowerCase) {
            testLogger.warn(s"!!! EXCEPTION !!! 'Source DDL' did not equal 'Target DDL' --- column datatype difference! *** FAILED ***")
            throw EqualSchemasException(s"'Source DDL' did not equal 'Target DDL' --- column data types difference! *** FAILED ***")
          }
        }
      } else {
        throw EqualSchemasException(s"'Source DDL' did not equal 'Target DDL' --- column count difference! *** FAILED ***")
      }
      testLogger.warn(s"!!! COMPLETE !!! 'Source DDL' equal 'Target DDL' --- column data types match! *** SUCCESS ***")
      testLogger.warn("*** END EqualSchemaTester... ***")
    }

    def currentConstantsTester(sourcePath: String, targetResPath: String): Unit = {
      testLogger.warn("*** START CurrentConstantsTester... ***")
      // [1,Клиент 1,Ф,-,2020-11-01] - строка, приведенная к элементу массива
      var file = Source.fromFile(sourcePath)
      val sourceResult = spark.sql(file.mkString)
      file.close()

      file = Source.fromFile(targetResPath)
      val targetRes = file.getLines().toArray
      file.close()

      targetRes.map(str => str.trim())
      val source = sourceResult.collect()

      if (source.length == targetRes.length) {
          for(row <- source) {
            if (targetRes.indexOf(row.toString()) == -1) {
              testLogger.warn(s"!!! EXCEPTION !!! 'Source SQL-query' did not equal 'Target Result' --- not equal data! *** FAILED ***")
              throw EqualQueryResultException(s"'Source SQL-query' did not equal 'Target Result' --- not equal data! *** FAILED ***")
            }
        }
      } else {
        testLogger.warn(s" !!! EXCEPTION !!! 'Source SQL-query' did not equal 'Target Result' --- not equal row count! *** FAILED ***")
        throw EqualQueryResultException(s"'Source SQL-query' did not equal 'Target Result' --- not equal row count! *** FAILED ***")
      }
      testLogger.warn(s" !!! COMPLETE !!! 'Source SQL-query' result equal 'Target Result'! *** SUCCESS ***")
      testLogger.warn("*** END CurrentConstantsTester... ***")
    }
  }
}
