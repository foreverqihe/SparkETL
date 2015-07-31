package com.servian.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import com.typesafe.scalalogging._
import org.slf4j.LoggerFactory

object DataLoader {

	def main(args:Array[String]) = {
		val logger = Logger(LoggerFactory.getLogger(this.getClass))

		val conf = new SparkConf().setAppName("Data Loader")

		val sc = new SparkContext(conf)
		val sqlContext = new SQLContext(sc)


		val options = Map(
				"driver" 	-> "org.postgresql.Driver",
				"url" 		-> "jdbc:postgresql://localhost/ds2?user=ds2&password=hadoop123",
				"dbtable"	-> "customers",
				"partitionColumn" -> "customerid",
				"lowerBound" -> "1",
				"upperBound" -> "20",
				"numPartitions" -> "1"
			)

		logger.warn("before load")
		val jdbcDataFrame = sqlContext.load("jdbc", options)
		jdbcDataFrame.registerTempTable("customers")
		jdbcDataFrame.write.parquet("hdfs:///user/hadoop/parquet_files/customers")
		val results = sqlContext.sql("SELECT count(firstname) FROM customers WHERE customerid < 10 order by customerid")
		results.map(t => "firstname: " + t(0) + " lastname: " + t(1)).collect().foreach(println)

		logger.warn("after load")

		sc.stop() 
	}
}