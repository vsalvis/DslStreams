name := "streams"

version := "1.0"

scalaVersion := "2.10.0-RC2"


resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "com.github.axel22" %% "scalameter" % "0.2"

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")