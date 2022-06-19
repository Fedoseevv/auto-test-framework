package com.testClass

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.io.Source

case class AutoTest(private val spark: SparkSession, private val settingPath: String) {
  private val results = mutable.Map.empty[String, Any]

  private def logTest(name: String, res: Boolean): Any = {
    val tmp = name.split("/").last.split("_")
    results += (s"${tmp(0)}_${tmp(1)}_${tmp(3).slice(0, tmp(3).length - 4)}" -> res)
  }

  def parseResult(): Any = {
    var flag = false
    for (item <- results) {
      println(s"${item._1} - *** ${if (item._2 == true) "COMPLETED" else "FAILED"} ***")
      if (item._2 == false)
        flag = true
    }
    if (flag)
      throw new Exception("Not all tests were successful! *** FAILED ***")
  }

  def isEqualCounts(countSourcePath: String, countTargetPath: String): Boolean = {
    var file = Source.fromFile(countSourcePath)
    val sourceCount = spark.sql(file.mkString).collect()(0)
      .toString().replace("[", "").replace("]", "").toInt
    file.close()

    file = Source.fromFile(countTargetPath)
    val targetCount: Int = spark.sql(file.mkString).collect()(0).toString()
      .replace("[", "").replace("]", "").toInt
    file.close()
    if (sourceCount != targetCount) {
      logTest(countSourcePath, res = false)
      false
    } else {
      logTest(countSourcePath, res = true)
      true
    }
  }

  def isEqualDF(sourceSQLPath: String, targetSQLPath: String): Boolean = {
    var file = Source.fromFile(sourceSQLPath)
    val sourceResult: DataFrame = spark.sql(file.mkString)

    file = Source.fromFile(targetSQLPath)
    val targetResult: DataFrame = spark.sql(file.mkString)
    file.close()
    val rowCountSource = sourceResult.count()
    val rowCountTarget = targetResult.count()
    val rowCountDiff: Long = Math.abs(rowCountSource - rowCountTarget)
    try {
      if (rowCountDiff == 0) {
        if (!targetResult.except(sourceResult).isEmpty) {
          logTest(sourceSQLPath, res = false)
          false
        } else {
          logTest(sourceSQLPath, res = true)
          true
        }
      } else {
        logTest(sourceSQLPath, res = false)
        false
      }
    } catch {
      case _: Throwable => {
        logTest(sourceSQLPath, res = false)
        false
      }
    }
  }

  def isEqualSchemas(sourceDDLPath: String, targetDDLPath: String): Boolean = {
    var file = Source.fromFile(sourceDDLPath)
    val sourceDDL = spark.sql(file.mkString)
    file = Source.fromFile(targetDDLPath)
    val targetDDL = spark.sql(file.mkString)
    file.close()

    sourceDDL.except(targetDDL).isEmpty
    if ((sourceDDL.count() - targetDDL.count() == 0 &&
      (sourceDDL.except(targetDDL).isEmpty))) {
      logTest(sourceDDLPath, res = true)
      true
    } else {
      logTest(sourceDDLPath, res = false)
      false
    }
  }

  def isCurrentConstants(sourcePath: String, targetResPath: String): Boolean = {
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
      for (row <- source) {
        if (targetRes.indexOf(row.toString()) == -1) {
          logTest(sourcePath, res = false)
          return false
        }
      }
    } else {
      logTest(sourcePath, res = false)
      return false
    }
    logTest(sourcePath, res = true)
    true
  }

}
