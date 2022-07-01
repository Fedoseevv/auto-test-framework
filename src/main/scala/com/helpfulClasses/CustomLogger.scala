package com.helpfulClasses

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
/**
 * Данный класс используется для логгирования результатов тестов. Экземпляр создается в классе AutoTest
 */
case class CustomLogger() {
  def info(msg: String): Unit = { // Логгирование SUCCESS-тестов, а также вывод другой информации
    val logDT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,ms").format(LocalDateTime.now)
    println(logDT + " INFO: " + msg)
  }
  def error(msg: String): Unit = { // Логгирование FAILED-тестов
    val logDT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,ms").format(LocalDateTime.now)
    println(logDT + " ERROR: " + msg)
  }
}
