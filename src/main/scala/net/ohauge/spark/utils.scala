package net.ohauge.spark

import org.apache.orc.impl.ConvertTreeReaderFactory.DateFromStringGroupTreeReader
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.DataFrame

object utils {

    def columnSuffix(suffix: String, excludedColumns: String*)(df: DataFrame): DataFrame = {
        val lookup = df.columns.filter(!excludedColumns.contains(_)).map(name => name -> s"${name}${suffix}").toMap
        lookup.foldLeft(df)((acc, ca) => acc.withColumnRenamed(ca._1, ca._2))
    }

    def diffByColumnName[T](otherDF: DataFrame, colName: String)(df: DataFrame): DataFrame = {
        val values = otherDF.collect.map(row => row.getAs[T](colName)).toList
        df.filter(!col(colName).isin(values:_*))
    }

    def structColumnToDataFrame(colName: String, colNames: String*)(df: DataFrame): DataFrame = {
        //val allColumnNames = colNames + s"${colName}"
        df.withColumn(colName, col(colName)).select(s"${colName}.*")
    }

    def structListColumnToDataFrame(colName: String)(df: DataFrame): DataFrame = {
        df.withColumn(colName, explode(col(colName))).select(s"${colName}.*")
    }
}
