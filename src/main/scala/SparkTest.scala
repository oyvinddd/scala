import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, column, expr, lead, lit, udf, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row, SparkSession}
import net.ohauge.spark.utils.{columnSuffix, diffByColumnName}

case class ValidationSource(id: Int, value1: String, value2: String)
case class Validation(id: Int, value1: String, value1_val: String, value2: String, value2_val: String)

object SparkTest extends App {
    // setup and initialize spark session object
    val spark = SparkSession.builder().master("local[1]").appName("SparkTest").getOrCreate()

    import spark.implicits._

    val a1 = Array((10, "1", "1", 9, null), (20, "3", "4", 8, null), (30, "1", "2", 7, null), (50, "d", "f", 4, null))
    val a2 = Array((10, "1", "1", 9, null), (20, "7", "8", 9, null), (30, "h", "d", 6, null), (40, "h", "g", 6, null))

    val sourceDF = spark.createDataset(a1).toDF("id", "v1", "v2", "v3", "v4").as("df2")
    val validationDF = spark.createDataset(a2).toDF("id", "v1", "v2", "v3", "v4").as("df1")

    sourceDF.transform(validate(validationDF)).show()

    def validate(validationDF: DataFrame)(df: DataFrame): DataFrame = {
        validationDF
          .transform(columnSuffix("Val", "id"))
          .join(df, "id")
          .transform(reorderColumns())
          .transform(addValidationColumn())
    }

    def addValidationColumn()(df: DataFrame): DataFrame = {
        // this condition checks if each row is valid according to the pairwise values of ech column
        val isRowValid = (1 to df.columns.length - 1 by 2)
          .map(i => checkIsValid(df, index = i))
          .reduce(_ && _)
        df.withColumn("Valid", when(isRowValid, true).otherwise(false))
    }

    def reorderColumns()(df: DataFrame): DataFrame = {
        val sortedColumns = df.columns.filter(_ != "id").sorted
        df.select("id", sortedColumns: _*)
    }

    def checkIsValid(df: DataFrame, index: Int): Column = {
        // checks if two particular columns has the same value
        val c1 = col(df.columns(index)) === col(df.columns(index+1))
        // checks if two particular columns both have null values
        val c2 = col(df.columns(index)).isNull && col(df.columns(index+1)).isNull
        c1 || c2
    }
}