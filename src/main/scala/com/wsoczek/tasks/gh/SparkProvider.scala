package com.wsoczek.tasks.gh

import org.apache.spark.sql.SparkSession

trait SparkProvider {
  def getSparkSession(appName: String): SparkSession
}

object LocalSparkProvider extends SparkProvider {
  override def getSparkSession(appName: String): SparkSession = {
    SparkSession.builder.
      master("local[*]").
      appName(appName).
      getOrCreate()
  }
}

object ProductionSparkProvider extends SparkProvider {
  override def getSparkSession(appName: String): SparkSession = ???
    // TODO: before deploying to production this provider should be implemented with proper configuration for cluster
}