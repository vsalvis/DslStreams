name := "streams"

version := "1.0"

scalaVersion := "2.10.0-M7"
//M7 works for lms
//RC5 works for scalameter

scalaOrganization := "org.scala-lang.virtualized"

resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += ScalaToolsSnapshots

libraryDependencies += "com.github.axel22" %% "scalameter" % "0.2"

libraryDependencies += "EPFL" %% "lms" % "0.3-SNAPSHOT"

scalacOptions += "-Yvirtualize"

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")



