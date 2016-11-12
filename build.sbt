lazy val commonSettings = Seq(
	name := "shenji-recomm",
	organization := "com.bj58",
	version := "0.1.0",
	scalaVersion := "2.10.5"
	)
	
val dependencies = Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.1" % "provided"
)

lazy val root = project.in(file(".")).
	settings(commonSettings: _*).
  	settings(
  		javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
  		// scalacOptions += "-target:jvm-1.6",
		libraryDependencies ++= dependencies,
		XitrumPackage.copy() // xitrum-package
	)
