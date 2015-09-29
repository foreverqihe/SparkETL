package com.servian.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object DataLoader {
	//val DRIVER = "org.postgresql.Driver"
	//val URL = "jdbc:postgresql://localhost/campaign?user=ds2&password=hadoop123"
	val DRIVER = "com.mysql.jdbc.Driver"
	val URL = "jdbc:mysql://db-hackathon.c3ctvtncfc4b.ap-southeast-2.rds.amazonaws.com/hackathon?user=hadoop&password=AFFDFF2012"

	def main(args:Array[String]) = {

		val conf = new SparkConf().setAppName("Data Loader")

		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)
		//val options = scala.collection.mutable.Map(
		//		"driver" 	-> DRIVER,
		//		"url" 		-> URL,
				//"dbtable"	-> "customer",
				//"partitionColumn" -> "customer_id",
				//"lowerBound" -> "1",
				//"upperBound" -> "1000000",
				//"numPartitions" -> "1"
		//	)

		val tables = Map("CUSTOMER" -> "CUSTOMER_ID",
						 "PRODUCT" -> "PRODUCT_ID",
						 "CAMPAIGN" -> "CAMPAIGN_ID",
						 "CONTACT_HISTORY" -> "HISTORY_ID",
						 "RESPONSE" -> "RESPONSE_ID",
						 "SERVICE" -> "SERVICE_ID"
			)
		tables.foreach{(table: (String, String)) => 
				val options = Map(
					"driver" -> DRIVER,
					"url" -> URL,
					"dbtable" -> table._1
					//"partitionColumn" -> table._2,
				)
				
				val jdbcDataFrame = sqlContext.read.format("jdbc").options(options).load()
				//jdbcDataFrame.registerTempTable(table._1)
				jdbcDataFrame.write.parquet("hdfs:///user/hadoop/parquet_files/" + table._1)
				//val results = sqlContext.sql("SELECT first_name, last_name FROM customer WHERE row_id < 10 order by customer_id")
				//results.map(t => "firstname: " + t(0) + " lastname: " + t(1)).collect().foreach(println)
			}

		sc.stop() 
	}
}
