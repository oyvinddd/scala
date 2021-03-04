package net.ohauge.spark

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame}

object utils {

    def columnSuffix(suffix: String, excludedColumns: String*)(df: DataFrame): DataFrame = {
        val lookup = df.columns.filter(!excludedColumns.contains(_)).map(name => name -> s"${name}${suffix}").toMap
        lookup.foldLeft(df)((acc, ca) => acc.withColumnRenamed(ca._1, ca._2))
    }

    def diffByColumnName[T](otherDF: DataFrame, colName: String)(df: DataFrame): DataFrame = {
        val values = otherDF.collect.map(row => row.getAs[T](colName)).toList
        df.filter(!col(colName).isin(values:_*))
    }
}
