name := "Data Loader"



lazy val commonSettings = Seq(
	scalaVersion := "2.11.6",
	version := "1.0",
	updateOptions := updateOptions.value.withCachedResolution(true),
	libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.4.0",
    "org.apache.spark" %% "spark-sql" % "1.4.0",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
    "ch.qos.logback" % "logback-classic" % "1.1.3",
    "org.postgresql" % "postgresql" % "9.3-1103-jdbc41"
   )
)

lazy val root = (project in file(".")).
	settings(commonSettings: _*).
	settings(

	)





