package net.ohauge.spark

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame}

object Utils {

    def columnSuffix(suffix: String, excludedColumns: String*)(df: DataFrame): DataFrame = {
        val lookup = df.columns.filter(!excludedColumns.contains(_)).map(name => name -> s"${name}${suffix}").toMap
        lookup.foldLeft(df)((acc, ca) => acc.withColumnRenamed(ca._1, ca._2))
    }

    def diffByColumnName[T](otherDF: DataFrame, colName: String)(df: DataFrame): DataFrame = {
        val values = otherDF.collect.map(row => row.getAs[T](colName)).toList
        df.filter(!col(colName).isin(values:_*))
    }

    def structColumnToDataFrame(colName: String, idColName: Option[String] = None, idColNewName: Option[String] = None, isList: Boolean = false)(df: DataFrame): DataFrame = {
        val structColumn = if(isList) explode(col(colName)) else col(colName)
        if(!idColName.isEmpty) {
            val newIDColName = s"${idColNewName.getOrElse(idColName.get)}"
            val allColumns = Seq(s"$colName.*", newIDColName).map(col(_))
            df.withColumn(newIDColName, col(idColName.get)).withColumn(colName, structColumn).select(allColumns:_*)
        } else {
            df.withColumn(colName, structColumn).select(s"$colName.*")
        }
    }
}
