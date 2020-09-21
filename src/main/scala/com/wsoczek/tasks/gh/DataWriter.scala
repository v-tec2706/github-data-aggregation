package com.wsoczek.tasks.gh

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object DataWriter {
  def saveToFile(dataFrame: DataFrame, partition: String, path: String): Unit = {
    val partitionColumnName = s"${partition}_part"
    dataFrame.coalesce(1)
      .withColumn(partitionColumnName, col(partition)) // partition column is duplicated to not miss it in output file
      .write
      .mode("overwrite")
      .partitionBy(partitionColumnName)
      .csv(path)
  }
}
