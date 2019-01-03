package apple.org.hadoop

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object KafkaCompact{
    def main(args: Array[String]){

        val inputDirectory = args(0)
        var outputDirectory = args(1)
        val year = args(2)
        val month = args(3)
        val day = args(4)

        outputDirectory = outputDirectory + "/" + year + "/" + month + "/" + day

        val spark = SparkSession
             .builder()
             .appName("Apple Kafka Query")
             .master("yarn")
             .config("spark.sql.orc.impl", "native")
             .config("spark.sql.streaming.fileSink.log.cleanupDelay", 60000)
             .getOrCreate()

        val defaultFSPath = spark.sparkContext.hadoopConfiguration.get("fs.defaultFS")

        import spark.implicits._

        val values = spark.read.format("orc").load(defaultFSPath + inputDirectory)

        values.printSchema

        val decoded = values.select("data", "year", "month", "day")
                            .filter($"day" === day.toInt)
                            .filter($"month" === month.toInt)
                            .filter($"year" === year.toInt)

        decoded.coalesce(10).write.format("orc").save(defaultFSPath + outputDirectory)

    }
}
