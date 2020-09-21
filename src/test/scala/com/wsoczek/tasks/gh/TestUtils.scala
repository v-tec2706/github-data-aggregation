package com.wsoczek.tasks.gh

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, types}
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait TestUtils extends FunSuite with SharedSparkContext with Checkers with DataFrameSuiteBase {

  import spark.implicits._

  def repositoryTestExpectedData: DataFrame = {
    val df = Seq((1L, "repo_name_1", 2L, 2L, 2L))
      .toDF("project_id", "project_name", "users_forking", "created_issues", "created_PRs")
      .withColumn("daily_date", to_date(lit("2020-01-01")))
      .select("daily_date", "project_id", "project_name", "users_forking", "created_issues", "created_PRs")

    val adjustedDFSchema = adjustDFSchema(df, Map("project_id" -> true))

    df.sqlContext.createDataFrame(df.rdd, adjustedDFSchema)
  }

  def repositoryWithStarTestExpectedData: DataFrame = {
    val df = Seq((2L, "repo_name_2", 1L, 1L, 1L, 0L), (1L, "repo_name_1", 2L, 2L, 2L, 1L))
      .toDF("project_id", "project_name", "users_forking", "created_issues", "created_PRs", "users_starring")
      .withColumn("daily_date", to_date(lit("2020-01-01")))
      .select("project_id", "daily_date", "project_name", "users_forking", "created_issues", "created_PRs", "users_starring")

    val adjustedDFSchema = adjustDFSchema(df, Map("project_id" -> true, "users_starring" -> true))

    df.sqlContext.createDataFrame(df.rdd, adjustedDFSchema)
  }

  def usersTestExpectedData: DataFrame = {
    val df = Seq((1L, "actor_login_1", 2L, 2L))
      .toDF("user_id", "user_login", "created_PRs_number", "created_issues_number")
      .withColumn("daily_date", to_date(lit("2020-01-01")))
      .select("user_id", "daily_date", "user_login", "created_PRs_number", "created_issues_number")

    val adjustedDFSchema = adjustDFSchema(df, Map("user_id" -> true))

    df.sqlContext.createDataFrame(df.rdd, adjustedDFSchema)
  }

  private def adjustDFSchema(df: DataFrame, nullableOptionForField: Map[String, Boolean]): StructType = {
    StructType(df.schema.map(x =>
      types.StructField(x.name, x.dataType, nullableOptionForField.getOrElse(x.name, x.nullable), x.metadata)))
  }

  def usersTestWithStarringExpectedData: DataFrame = {
    val df = Seq((1L, "actor_login_1", 2L, 2L, 1L), (2L, "actor_login_2", 1L, 1L, 0L))
      .toDF("user_id", "user_login", "created_PRs_number", "created_issues_number", "starred_projects")
      .withColumn("daily_date", to_date(lit("2020-01-01")))
      .select("user_id", "daily_date", "user_login", "created_PRs_number", "created_issues_number", "starred_projects")

    val adjustedDFSchema = adjustDFSchema(df, Map("user_id" -> true, "starred_projects" -> true))

    df.sqlContext.createDataFrame(df.rdd, adjustedDFSchema)
  }
}
