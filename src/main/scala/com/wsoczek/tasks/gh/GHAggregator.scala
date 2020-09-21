package com.wsoczek.tasks.gh

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

trait Aggregator {
  def performAggregation(df: DataFrame): DataFrame
}

case class RepositoryDataAggregator() extends Aggregator {
  override def performAggregation(df: DataFrame): DataFrame = {
    df.groupBy(to_date(col("created_at")) as "daily_date",
      col("repo.id") as "project_id",
      col("repo.name") as "project_name")
      .agg(countDistinct(col("payload.forkee.owner.id")) as "users_forking",
        countDistinct(col("payload.issue.id")) as "created_issues",
        countDistinct(col("payload.pull_request.id")) as "created_PRs")
  }
}

case class UserDataAggregator() extends Aggregator {
  override def performAggregation(df: DataFrame): DataFrame = {
    val userAggregationBaseDF = df.filter((col("actor.id") equalTo col("payload.pull_request.user.id"))
      or (col("actor.id") equalTo col("payload.issue.user.id")))
      .groupBy(to_date(col("created_at")) as "daily_date",
        col("actor.id") as "user_id",
        col("actor.login") as "user_login")

    val pullRequestDF = userAggregationBaseDF.agg(countDistinct(col("payload.pull_request.id")) as "created_PRs_number")
    val issuesDF = userAggregationBaseDF.agg(countDistinct(col("payload.issue.id")) as "created_issues_number")
      .drop("issue_user_id", "daily_date", "user_login", "PR_user_id")

    pullRequestDF.join(issuesDF, "user_id")
      .drop("PR_user_id", "issue_user_id")
  }
}


/*
Above implementations don't contain aggregations based on `star`. If my understanding is correct staring activities should have `starred_at` field. Unfortunately I didn't find such a field
in data I fetched. I will implement another set of aggregators, which will calculate star metrics basing on my assumption on this fields. Then I will mock some data, and check results.
 */

case class RepositoryDataAggregatorWithStarring() extends Aggregator {
  override def performAggregation(df: DataFrame): DataFrame = {
    val repositoryDataAggregation = RepositoryDataAggregator().performAggregation(df)
    val starringDataAggregation = df.filter(col("starred_at") isNotNull)
      .groupBy(to_date(col("created_at")) as "daily_date",
        col("repo.id") as "project_id",
        col("repo.name") as "project_name")
      .agg(countDistinct(col("sender.id")) as "users_starring_tmp")
      .drop("daily_date", "project_name")

    val fullDataAggregation = repositoryDataAggregation.join(starringDataAggregation, Seq("project_id"), "left")
    fullDataAggregation.withColumn("users_starring",
      when(col("users_starring_tmp").isNull, 0).otherwise(col("users_starring_tmp")))
      .drop(col("users_starring_tmp"))
  }
}

case class UserDataAggregatorWithStarring() extends Aggregator {
  override def performAggregation(df: DataFrame): DataFrame = {
    val userDataAggregation = UserDataAggregator().performAggregation(df)
    val userDataWithStarringAggregation = df.filter(col("starred_at").isNotNull)
      .groupBy(to_date(col("created_at")) as "daily_date",
        col("sender.id") as "user_id")
      .agg(count(col("starred_at")) as "starred_projects_tmp")
      .drop("daily_date")

    val fullDataAggregation = userDataAggregation.join(userDataWithStarringAggregation, Seq("user_id"), "left")
    fullDataAggregation.withColumn("starred_projects",
      when(col("starred_projects_tmp").isNull, 0).otherwise(col("starred_projects_tmp")))
      .drop(col("starred_projects_tmp"))
  }
}
