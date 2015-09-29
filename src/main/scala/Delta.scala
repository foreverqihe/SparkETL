package com.servian.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.Path

object DeltaLoader {
	//val DRIVER = "org.postgresql.Driver"
	//val URL = "jdbc:postgresql://localhost/campaign?user=ds2&password=hadoop123"
	val DRIVER = "com.mysql.jdbc.Driver"
	val URL = "jdbc:mysql://db-hackathon.c3ctvtncfc4b.ap-southeast-2.rds.amazonaws.com/hackathon?user=hadoop&password=AFFDFF2012"

	def main(args:Array[String]) = {

		val conf = new SparkConf().setAppName("Delta")
    val hdConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://ec2-54-66-13-249.ap-southeast-2.compute.amazonaws.com:9000"), hdConf)

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
						// "PRODUCT" -> "PRODUCT_ID",
						 "CAMPAIGN" -> "CAMPAIGN_ID",
						//"RESPONSE" -> "RESPONSE_ID",
						 "SERVICE" -> "SERVICE_ID"
			)
		tables.foreach{(table: (String, String)) => 
			  val delta_table = table._1 + "_DELTA"	
        
        val options = Map(
					"driver" -> DRIVER,
					"url" -> URL,
					"dbtable" -> delta_table
					//"partitionColumn" -> table._2,
				)
				
				val delta = sqlContext.read.format("jdbc").options(options).load().drop("ROW_ID").as("delta")
        val old = sqlContext.read.parquet("hdfs:///user/hadoop/parquet_files/" + table._1).drop("ROW_ID").as("old")
        val oldId = old.select(table._2)
        val dtId = delta.select(table._2)
        val changed = delta.join(oldId, table._2)
        val unchanged = old.join(oldId.except(dtId), table._2)
        println("unchanged: " + unchanged.count())
        println("changed: " + changed.count())
        val added = delta.join(dtId.except(oldId), table._2)
        println("added: " + added.count())
        val total = unchanged.unionAll(changed).unionAll(added).cache()
        val dateFormat = new java.text.SimpleDateFormat("yyyyMMddhhmmss")
        val date  = new java.util.Date()
        val dateStr = dateFormat.format(date)
        println("total: " + total.count())
        val output = "hdfs:///user/hadoop/parquet_files/" + table._1 
        val archive = output + ".archive"
        val temp = 
        try {
          val outPath = new Path(output)
          val arcPath = new Path(archive)
          if (hdfs.isDirectory(outPath) || hdfs.isFile(outPath)) {
            if (hdfs.isDirectory(arcPath) || hdfs.isFile(arcPath)) {
              hdfs.delete(arcPath, true)
            }
            hdfs.rename(outPath, new Path(archive))
          }
          total.write.parquet(output)
        } catch {
          case _: Throwable => {}
        }
			}

		sc.stop() 
	}
}
