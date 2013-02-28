name := "streams"

version := "1.0"

scalaVersion := "2.10.0"
//M7 works for lms
//RC5 works for scalameter

scalaOrganization := "org.scala-lang.virtualized"

resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += ScalaToolsSnapshots

libraryDependencies += "com.github.axel22" %% "scalameter" % "0.2"

libraryDependencies += "org.scala-lang.virtualized" % "scala-library" % "2.10.0"

libraryDependencies += "org.scala-lang.virtualized" % "scala-compiler" % "2.10.0"

libraryDependencies += "org.scala-lang" % "scala-actors" % "2.10.0" // for ScalaTest

libraryDependencies ++= Seq(
    "org.scalatest" % "scalatest_2.10" % "2.0.M5b" % "test",
    "EPFL" % "lms_2.10.0" % "0.3-SNAPSHOT")

//libraryDependencies += "EPFL" %% "lms" % "0.3-SNAPSHOT"
//libraryDependencies += "EPFL" % "lms_2.10.0" % "0.3-SNAPSHOT"

//libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"
//http://mvnrepository.com/artifact/org.scalatest/scalatest_2.10.0-M7/1.9-2.10.0-M7-B1
//libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0.M5b" % "test"

scalacOptions += "-Yvirtualize"

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")



