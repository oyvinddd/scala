package net.ohauge.spark

import org.apache.orc.impl.ConvertTreeReaderFactory.DateFromStringGroupTreeReader
import org.apache.spark.sql.functions.{col, column, explode, lit}
import org.apache.spark.sql.{Column, DataFrame}
import org.glassfish.jersey.internal.util.collection.StringIgnoreCaseKeyComparator

object Utils {

    def columnSuffix(suffix: String, excludedColumns: String*)(df: DataFrame): DataFrame = {
        val lookup = df.columns.filter(!excludedColumns.contains(_)).map(name => name -> s"${name}${suffix}").toMap
        lookup.foldLeft(df)((acc, ca) => acc.withColumnRenamed(ca._1, ca._2))
    }

    def diffByColumnName[T](otherDF: DataFrame, colName: String)(df: DataFrame): DataFrame = {
        val values = otherDF.collect.map(row => row.getAs[T](colName)).toList
        df.filter(!col(colName).isin(values:_*))
    }

    def structColumnToDataFrame(colName: String, prefix: String, additionalColNames: String*)(df: DataFrame): DataFrame = {
        val allColNames: Seq[String] = additionalColNames :+ s"${colName}.*"
        val allCols: Seq[Column] = allColNames.map(col(_))
        df.withColumn(colName, col(colName)).select(allCols:_*)
    }

    def structColumnToDataFrame2(colName: String, idColName: Option[String], idColPrefix: Option[String])(df: DataFrame): DataFrame = {
        if(!idColName.isEmpty) {
            val newIDColName = s"${idColPrefix.get}${idColName.getOrElse("")}"
            val allColumns: Seq[Column] = Array(s"$colName.*", newIDColName).map(col(_))
            df.withColumn(newIDColName, col(idColName.get)).withColumn(colName, col(colName)).select(allColumns:_*)
        } else {
            df.withColumn(colName, col(colName)).select(s"$colName.*")
        }
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
