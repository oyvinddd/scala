package net.ohauge.spark

import org.apache.orc.impl.ConvertTreeReaderFactory.DateFromStringGroupTreeReader
import org.apache.spark.sql.functions.{col, column, explode}
import org.apache.spark.sql.{Column, DataFrame}

object Utils {

    def columnSuffix(suffix: String, excludedColumns: String*)(df: DataFrame): DataFrame = {
        val lookup = df.columns.filter(!excludedColumns.contains(_)).map(name => name -> s"${name}${suffix}").toMap
        lookup.foldLeft(df)((acc, ca) => acc.withColumnRenamed(ca._1, ca._2))
    }

    def diffByColumnName[T](otherDF: DataFrame, colName: String)(df: DataFrame): DataFrame = {
        val values = otherDF.collect.map(row => row.getAs[T](colName)).toList
        df.filter(!col(colName).isin(values:_*))
    }

    def structColumnToDataFrame(colName: String, additionalColNames: String*)(df: DataFrame): DataFrame = {
        val allColNames: Seq[String] = additionalColNames :+ s"${colName}.*"
        val allCols: Seq[Column] = allColNames.map(col(_))
        df.withColumn(colName, col(colName)).select(allCols:_*)
    }

    def structListColumnToDataFrame(colName: String, additionalColNames: String*)(df: DataFrame): DataFrame = {
        val allColNames: Seq[String] = additionalColNames :+ s"${colName}.*"
        val allCols: Seq[Column] = allColNames.map(col(_))
        df.withColumn(colName, explode(col(colName))).select(allCols:_*)
    }

    def dd(colName: String, colNames: String*)(df: DataFrame): DataFrame = {
        val allColNames = colNames :+ colName
        val columns: Seq[Column] = allColNames.map(col(_))
        df.select(columns:_*)
    }
}
