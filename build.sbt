name := "Auto-test framework"

version := "0.1"

scalaVersion := "2.12.15"
//scalaVersion := "2.11.5"

val sparkVersion = "2.4.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
  case n if n.contains("services") => MergeStrategy.concat
}
assemblyJarName in assembly := "AutoTest-bank.jar"
