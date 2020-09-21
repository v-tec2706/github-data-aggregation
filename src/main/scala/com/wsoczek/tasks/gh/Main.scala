package com.wsoczek.tasks.gh

import com.wsoczek.tasks.gh.DataFrameUtils._

import scala.util.{Failure, Success, Try}

object Main extends App {
  val inputPath = "./data/gharchive.json.gz"
  val outputPaths: Map[Aggregator, String] = Map(
    RepositoryDataAggregator() -> "./data/repository_agg_result",
    RepositoryDataAggregatorWithStarring() -> "./data/repository_agg_result",
    UserDataAggregator() -> "./data/user_repository_result",
    UserDataAggregatorWithStarring() -> "./data/user_repository_result"
  )
  val partitionName = "daily_date"

  val spark = LocalSparkProvider.getSparkSession(appName = "GH_aggregation_task")
  val dataDF = spark.read.json(inputPath)

  val aggregators = hasColumn(dataDF, "starred_at") match {
    case true => Seq(RepositoryDataAggregatorWithStarring(), UserDataAggregatorWithStarring())
    case false => Seq(RepositoryDataAggregator(), UserDataAggregator())
  }

  val aggregations = aggregators.map(x => (x, Try(x.performAggregation(dataDF))))

  aggregations.foreach {
    case (aggregator, Success(dataFrame)) => DataWriter.saveToFile(dataFrame, partitionName, outputPaths(aggregator))
    case (_, Failure(exception)) =>
      println(s"Problem with aggregations calculation. Error: $exception") // FIXME: more specific error matching should be provided here
  }
}