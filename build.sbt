//scalaVersion := "2.12.8"
//
//name := "stackoverflow"
//scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
//
////libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
//
//libraryDependencies ++= Seq(
//  ("org.apache.spark" %% "spark-core" % "2.4.3"),
//  "org.apache.spark" %% "spark-mllib" % "2.4.8"
//)

scalaVersion := "2.12.8"

name := "stackoverflow"
scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")

//libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "3.2.1")
)

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.1"
