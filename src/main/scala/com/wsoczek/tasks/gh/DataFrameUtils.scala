package com.wsoczek.tasks.gh

import org.apache.spark.sql.DataFrame

import scala.util.Try

object DataFrameUtils {
  def hasColumn(df: DataFrame, columnName: String): Boolean = Try(df(columnName)).isSuccess
}
