import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{lead, lit, udf}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row, SparkSession}
import scala.reflect.runtime.universe._

case class ValidationSource(id: Int, value1: String, value2: String)
case class Validation(id: Int, value1: String, value1_val: String, value2: String, value2_val: String)

object SparkTest extends App {
    // setup and initialize spark session object
    val spark = SparkSession.builder().master("local[1]").appName("SparkTest").getOrCreate()

    import spark.implicits._

    val a1 = Array((10, "2", "3"), (20, "4", "5"), (30, "10", "20"))
    val a2 = Array((10, "2", "3"), (20, "8", "9"))

    val df1 = spark.createDataset(a1).toDF("id", "value1", "value2")
    val df2 = spark.createDataset(a2).toDF("id", "value1", "value2")

    val rows: DataFrame = df1.select("value1").where($"id" === 0)
    if(rows.count() == 1) {
        println(rows.first.get(0))
    }

    def unionByID(otherDF: DataFrame)(df: DataFrame): DataFrame = {
        val ids = otherDF.select("id").map(r => r(0).asInstanceOf[Int]).collect
        df.filter(r => ids.contains(r(0)))
    }

    def diffByColumnName(df1: DataFrame, df2: DataFrame, columnName: String = "id"): DataFrame = {
        val ids: Array[Int] = df2.select(columnName).map(row => row(0).asInstanceOf[Int]).collect()
        df1.filter(row => !ids.contains(row(0)))
    }

    def unionByColumnName(df1: DataFrame, df2: DataFrame, columnName: String = "id"): DataFrame = {
        val ids: Array[Int] = df1.select(columnName).map(row => row(0).asInstanceOf[Int]).collect()
        df2.filter(row => ids.contains(row(0)))
    }

    def validate(validationDF: DataFrame)(df: DataFrame) = {
        val dd = validationDF.col("id")
        df.withColumn("IsValid", lit(false))
          .withColumn("testing", dd)
    }

    def createMap(df: DataFrame): Map[Int, Seq[String]] = {
        df.map(r => r(0).asInstanceOf[Int] -> Seq(r.getString(0), r.getString(1), r.getString(2))).collect.toMap
    }

    def valueFromMap(map: Map[Int, Seq[Any]], listIndex: Int) = udf {
        (key: Int) => map(key)(listIndex).asInstanceOf[String]
    }

    def validationValue(validationDF: DataFrame, id: String, colName: String)(df: DataFrame): DataFrame = {
        val row = validationDF.select(colName).where($"id".equalTo(id)).first
        val newColName = "%s_validation".format(colName)
        df.withColumn(newColName, lit(row.get(0)))
    }
}