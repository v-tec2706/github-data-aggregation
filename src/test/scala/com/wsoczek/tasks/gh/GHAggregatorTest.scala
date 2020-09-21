package com.wsoczek.tasks.gh

class GHAggregatorTest extends TestUtils {

  test("Repository data aggregation") {

    val aggregator = new RepositoryDataAggregator
    val inputDF = spark.read.json("./src/test/resources/repositoryTestData.json")

    val expectedDF = repositoryTestExpectedData
    val actualDF = aggregator.performAggregation(inputDF)
    assertDataFrameEquals(expectedDF, actualDF)
  }

  test("User data aggregation") {
    val aggregator = new UserDataAggregator
    val inputDF = spark.read.json("./src/test/resources/userTestData.json")

    val actualDF = aggregator.performAggregation(inputDF)
    val expectedDF = usersTestExpectedData

    assertDataFrameEquals(expectedDF, actualDF)
  }

  test("Repository data with `starred_at` data aggregation") {

    val aggregator = new RepositoryDataAggregatorWithStarring
    val inputDF = spark.read.json("./src/test/resources/repositoryTestDataWithStarring.json")

    val expectedDF = repositoryWithStarTestExpectedData
    val actualDF = aggregator.performAggregation(inputDF)
    assertDataFrameEquals(expectedDF, actualDF)
  }

  test("User data with `starred_at` aggregation") {
    val aggregator = new UserDataAggregatorWithStarring
    val inputDF = spark.read.json("./src/test/resources/userTestDataWithStarring.json")

    val actualDF = aggregator.performAggregation(inputDF)
    val expectedDF = usersTestWithStarringExpectedData

    assertDataFrameEquals(expectedDF, actualDF)
  }
}
