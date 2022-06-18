//import com.helpfulClasses.{CountException, EqualDfException, EqualQueryResultException, EqualSchemasException}
//import org.apache.log4j.LogManager
//import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
//import org.codehaus.jettison.json.JSONArray
//import org.scalatest.funsuite.AnyFunSuite
//
//import scala.collection.mutable
//import scala.io.Source
//
//object DemoTester_v2 {
//  case class Tester_v2(private val spark: SparkSession, private val settingPath: String) extends AnyFunSuite {
//
//    def startTesting(): Unit = {
//      val file = Source.fromFile(settingPath)
//      val allTests: JSONArray = new JSONArray(file.mkString)
//      file.close()
//      this.execute()
//      for (i <- 0 until  allTests.length()) {
//        val currentTest = allTests.getJSONObject(i) // Рассматриваем i-ый тест
//        currentTest.get("type") match {
//          case "counts" => this.countDemoTester(currentTest.get("source_path").toString,
//            currentTest.get("target_path").toString)
//          case "dataframes" => this.equalDFTester(currentTest.get("source_path").toString,
//            currentTest.get("target_path").toString)
//          case "schema" => this.equalSchemaTest(currentTest.get("source_path").toString,
//            currentTest.get("target_showcase").toString)
//          case "constants" => this.currentQueryRes(currentTest.get("source_path").toString,
//            currentTest.get("constants_path").toString)
//          case _ => throw new Exception("Invalid test type. Check your test file!!! *** FAILED ***")
//
//        }
//      }
//    }
//
//    def countDemoTester(countSourcePath: String, countTargetPath: String): Unit = {
//      var file = Source.fromFile(countSourcePath)
//      val sourceCount = spark.sql(file.mkString).count()
//      file.close()
//      file = Source.fromFile(countTargetPath)
//      val targetCount  = spark.sql(file.mkString).count()
//      file.close()
//
//      test("Row count source sql-query should be equal row count target sql-query") {
//        assert(sourceCount == targetCount)
//      }
//    }
//
//    def equalDFTester(sourceSQLPath: String, targetSQLPath: String): Unit = {
//      var file = Source.fromFile(sourceSQLPath)
//      val sourceResult: DataFrame = spark.sql(file.mkString)
//      file.close()
//
//      file = Source.fromFile(targetSQLPath)
//      val targetResult: DataFrame = spark.sql(file.mkString)
//      file.close()
//      val rowCountSource = sourceResult.count()
//      val rowCountTarget = targetResult.count()
//      var rowCountDiff: Long = Math.abs(rowCountSource - rowCountTarget)
//      try {
//        if (rowCountDiff == 0) {
//          val difference = targetResult.except(sourceResult).count
//          if (difference != 0) { //
//            throw EqualDfException()
//          }
//        }
//      } catch {
//        case ex: EqualDfException => throw EqualDfException(s"'Source DF' did not equal 'Target DF' --- row count difference: ${rowCountDiff}. *** FAILED ***")
//        case ex: AnalysisException => throw EqualDfException(s"'Source DF schema' did not equal 'Target DF schema'! *** FAILED ***")
//        case _ => throw new Exception("Другой вид ошибки! *** FAILED ***")
//      }
//    }
//
//    def equalSchemaTest(sourceDDLPath: String, targetShowcaseName: String): Unit = {
//      val showcaseDescribe = spark.sql(s"DESCRIBE $targetShowcaseName")
//        .drop("Comment")
//
//      val file = Source.fromFile(sourceDDLPath)
//      val sourceDDL = file.mkString.toLowerCase
//      file.close()
//
//      val mapDDL = mutable.Map[String, String]()
//      sourceDDL.split(",").foreach(item => {
//        val tmp = item.split("` ")
//        mapDDL += (tmp(0).replace("`", "") -> tmp(1).replaceAll(" ", ""))
//      })
//
//
//      if (mapDDL.size == showcaseDescribe.count()) {
//        for(row <- showcaseDescribe) {
//          val tmp = row.toString().replace("[", "").replace("]", "").split(",")
//          if (mapDDL(tmp(0).toLowerCase) != tmp(1).toLowerCase) {
//            throw EqualSchemasException(s"'Source DDL' did not equal 'Target DDL' --- column datatype difference! *** FAILED ***")
//          }
//        }
//      } else {
//        throw EqualSchemasException(s"'Source DDL' did not equal 'Target DDL' --- column count difference! *** FAILED ***")
//      }
//    }
//
//    def currentQueryRes(sourcePath: String, targetResPath: String): Unit = {
//      // [1,Клиент 1,Ф,-,2020-11-01] - строка, приведенная к элементу массива
//      var file = Source.fromFile(sourcePath)
//      val sourceResult = spark.sql(file.mkString)
//      file.close()
//
//      file = Source.fromFile(targetResPath)
//      val targetRes = file.getLines().toArray
//      file.close()
//
//      targetRes.map(str => str.trim())
//      val source = sourceResult.collect()
//
//      if (source.length == targetRes.length) {
//        for(row <- source) {
//          if (targetRes.indexOf(row.toString()) == -1) {
//            throw EqualQueryResultException(s"'Source SQL-query' did not equal 'Target Result' --- not equal data! *** FAILED ***")
//          }
//        }
//      } else {
//        throw EqualQueryResultException(s"'Source SQL-query' did not equal 'Target Result' --- not equal row count! *** FAILED ***")
//      }
//    }
//  }
//}
