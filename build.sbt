lazy val commonSettings = Seq(
	name := "shenji-recomm",
	organization := "com.bj58",
	version := "0.1.0",
	scalaVersion := "2.11.8"
	)
	
val dependencies = Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-mllib_2.11" % "1.6.1" % "provided"
)

lazy val root = project.in(file(".")).
	settings(commonSettings: _*).
  	settings(
		libraryDependencies ++= dependencies,
		XitrumPackage.copy() // xitrum-package
	)

