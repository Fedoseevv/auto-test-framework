package com.main

import scala.io.Source

object tmp {
  def main(args: Array[String]): Unit = {
    val name = "/test_3_source_arrays.sql"
    val source = getClass.getResourceAsStream(name)
    val query = Source.fromInputStream(source).mkString
    println(query)
    val targetSource = getClass.getResourceAsStream(name.replace("source", "target"))
    val targetQuery = Source.fromInputStream(targetSource).mkString
    println(targetQuery)

  }
}
