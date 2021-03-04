import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, column, lead, lit, udf, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, Row, SparkSession}
import net.ohauge.spark.utils.{columnSuffix, diffByColumnName}

case class ValidationSource(id: Int, value1: String, value2: String)
case class Validation(id: Int, value1: String, value1_val: String, value2: String, value2_val: String)

object SparkTest extends App {
    // setup and initialize spark session object
    val spark = SparkSession.builder().master("local[1]").appName("SparkTest").getOrCreate()

    import spark.implicits._

    val a1 = Array((10, "0", "1"), (20, "3", "4"), (30, "11", "12"), (50, "d", "f"))
    val a2 = Array((10, "1", "1"), (20, "7", "8"), (30, "h", "d"), (40, "h", "g"))

    val sourceDF = spark.createDataset(a1).toDF("id", "value1", "value2").as("df2")
    val validationDF = spark.createDataset(a2).toDF("id", "value1", "value2").as("df1")

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
        val isRowValid = (1 to df.columns.length - 1 by 2).map(i => (col(df.columns(i)) === col(df.columns(i+1)))).reduce(_ && _)
        df.withColumn("IsValid", when(isRowValid, true).otherwise(false))
    }

    def reorderColumns()(df: DataFrame): DataFrame = {
        val sortedColumns = df.columns.filter(_ != "id").sorted
        df.select("id", df.columns.filter(_ != "id").sorted: _*)
    }
}