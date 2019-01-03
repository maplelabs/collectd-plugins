package snappyflow.org.hadoop

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object KafkaStream{
    def main(args: Array[String]){

        val spark = SparkSession
             .builder()
             .appName("Kafka Stream")
             .master("yarn")
             .config("spark.sql.orc.impl", "native")
             .config("spark.sql.streaming.fileSink.log.cleanupDelay", 60000)
             .getOrCreate()

        val defaultFSPath = spark.sparkContext.hadoopConfiguration.get("fs.defaultFS")
        val outputDirectory = args(0)
        val checkpointDirectory = args(1)
        val brokerServers = args(2)
        val brokerTopic = args(3)

        import spark.implicits._

        //val servers = "broker-ip:port"

        val df = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", brokerServers)
            .option("subscribe", brokerTopic)
            .option("failOnDataLoss", false)
            .load()

        val logSchema = StructType(Array(
	    StructField("node", StringType),
	    StructField("_documentType", StringType),
	    StructField("_plugin", StringType),
	    StructField("level", StringType),
	    StructField("time", TimestampType),
	    StructField("_tag_appName", StringType),
	    StructField("message", StringType)))

        val values = df.selectExpr("CAST (value as string) as json")
                     .select(from_json($"json", schema=logSchema).as("data"))

        val output = values
                     .withColumn("year", year(values("data.time")))
                     .withColumn("month", month(values("data.time")))
                     .withColumn("day", dayofmonth(values("data.time")))

        output.printSchema

        output.writeStream
            .trigger(ProcessingTime("30 minutes"))
            .outputMode("append")
            .format("orc")
            .partitionBy("year", "month", "day")
            .option("checkpointLocation", defaultFSPath + checkpointDirectory)
            .option("path", defaultFSPath + outputDirectory)
            .start()
            .awaitTermination()

    }
}
