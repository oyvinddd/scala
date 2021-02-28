//import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lead, lit}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object SparkTest extends App {
    // setup and initialize spark session object
    val spark = SparkSession.builder().master("local[1]").appName("SparkTest").getOrCreate()

    import spark.implicits._

    val a1 = Array(("1", "2", "3"), ("4", "5", "6"))
    val a2 = Array(("1", "2", "3"), ("4", "5", "6"))

    val df1 = spark.createDataset(a1).toDF("c1", "c2", "c3")
    val df2 = spark.createDataset(a2).toDF("c1", "c2", "c3")
    df1.show
    df2.show


}