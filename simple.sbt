name := "Data Loader"



lazy val commonSettings = Seq(
	scalaVersion := "2.10.4",
	version := "1.0",
	updateOptions := updateOptions.value.withCachedResolution(true),
	libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.4.0",
    "org.apache.spark" %% "spark-sql" % "1.4.0",
    "org.postgresql" % "postgresql" % "9.3-1103-jdbc41",
	"mysql" % "mysql-connector-java" % "5.1.36"
   )
)

lazy val root = (project in file(".")).
	settings(commonSettings: _*).
	settings(

	)





