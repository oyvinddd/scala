package net.ohauge.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

class Utils {

    val spark = SparkSession.builder().master("local[1]").appName("SparkTest").getOrCreate()
    import spark.implicits._

    def createDataFrame(a: Array[String]): DataFrame = {
        spark.createDataset(a).toDF()
    }
}
