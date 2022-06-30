package com.helpfulClasses

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

case class CustomLogger() {
  def info(msg: String): Unit = {
    val logDT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,ms").format(LocalDateTime.now)
    println(Console.GREEN + logDT + " INFO: " + msg)
  }
  def error(msg: String): Unit = {
    val logDT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,ms").format(LocalDateTime.now)
    println(Console.RED + logDT + " ERROR: " + msg)
  }
}
