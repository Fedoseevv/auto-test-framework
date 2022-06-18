package com.functions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.codehaus.jettison.json.JSONArray

import java.net.URI
import scala.collection.immutable.Stream

object ConfigReader {
  def getConfigPath(confPath: String, URI: String, target: String): String = {
    val hdfs = FileSystem.get(new URI(URI), new Configuration())
    val path = new Path(confPath)
    val stream = hdfs.open(path)
    var res = ""
    def readLines: Stream.Cons[String] = Stream.cons(stream.readLine, Stream.continually( stream.readLine))
    val json = new JSONArray(readLines.mkString)

    json.getJSONObject(0).get(target).toString
  }
}