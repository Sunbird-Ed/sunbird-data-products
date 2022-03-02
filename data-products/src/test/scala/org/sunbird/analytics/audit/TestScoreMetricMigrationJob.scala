package org.sunbird.analytics.audit

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.{BaseSpec, EmbeddedCassandra}

class TestScoreMetricMigrationJob extends BaseSpec with MockFactory {
  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    spark = getSparkSession()
    EmbeddedCassandra.loadData("src/test/resources/score-metrics-migration/data.cql")
  }

  override def afterEach(): Unit = {
    spark.close()
    EmbeddedCassandra.close()
  }

  it should "Should migrate the score metrics data" in {
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.audit.ScoreMetricMigrationJob","modelParams":{"store":"azure","sparkCassandraConnectionHost":"localhost", "fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')", "metricsType":"attempt_metrics", "forceMerge": false},"parallelization":8,"appName":"ScoreMetricMigrationJob"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val sc: SparkContext = spark.sparkContext
    val res = ScoreMetricMigrationJob.migrateData(spark, jobConfig)
    val result = res.filter(col("context_id") === "cb:batch-001")
      .filter(col("activity_id") === "do_11306040245271756813015")
      .filter(col("user_id") === "user-008")
      .select("aggregates", "agg_last_updated", "agg_details").collect()
    result.head.get(0).asInstanceOf[Map[String, Int]]("completedCount") should be(0)
    result.head.get(0).asInstanceOf[Map[String, Int]]("score:do_112876961957437440179") should be(10)
    result.head.get(0).asInstanceOf[Map[String, Int]]("max_score:do_112876961957437440179") should be(10)
    result.head.get(0).asInstanceOf[Map[String, Int]]("attempts_count:do_112876961957437440179") should be(1)

    result.head.get(1).asInstanceOf[Map[String, Int]].keySet should contain allElementsOf List("completedCount", "score:do_112876961957437440179", "max_score:do_112876961957437440179", "attempts_count:do_112876961957437440179")

    result.head.get(2).asInstanceOf[Seq[String]].size should be (2)

    result.head.get(2).asInstanceOf[Seq[String]] should contain allElementsOf List("""{"max_score":10.0,"score":10.0,"type":"attempt_metrics","attempt_id":"attempat-001","content_id":"do_112876961957437440110","attempted_on":1634810023}""", """{"max_score":10.0,"score":10.0,"type":"attempt_metrics","attempt_id":"attempat-001","content_id":"do_112876961957437440179"}""")

    val result2 = res.filter(col("context_id") === "cb:batch-001")
      .filter(col("activity_id") === "do_11306040245271756813015")
      .filter(col("user_id") === "user-010")
      .select("agg_details").collect()

    result2.head.get(0).asInstanceOf[Seq[String]].size should be (2)

    result2.head.get(0).asInstanceOf[Seq[String]] should contain allElementsOf List("""{"max_score":15.0,"score":15.0,"type":"attempt_metrics","attempt_id":"attempat-001","content_id":"do_11307593493010022418"}""", """{"max_score":15.0,"score":10.0,"type":"attempt_metrics","attempt_id":"attempat-002","content_id":"do_11307593493010022418"}""")

    ScoreMetricMigrationJob.updatedTable(res, ScoreMetricMigrationJob.userActivityAggDBSettings)

    val result3 = res.filter(col("context_id") === "cb:batch-001")
      .filter(col("activity_id") === "do_1130928636168192001667")
      .filter(col("user_id") === "user-003")
      .select("aggregates").collect()

    result3.head.get(0).asInstanceOf[Map[String, Int]]("completedCount") should be(1)
    result3.head.get(0).asInstanceOf[Map[String, Int]]("score:do_112876961957437440179") should be(7.5)
    result3.head.get(0).asInstanceOf[Map[String, Int]]("max_score:do_112876961957437440179") should be(10)
    result3.head.get(0).asInstanceOf[Map[String, Int]]("attempts_count:do_112876961957437440179") should be(2)
  }

  it should "Should migrate the score metrics data for specific batch" in {
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.audit.ScoreMetricMigrationJob","modelParams":{"store":"azure","sparkCassandraConnectionHost":"localhost", "fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')", "batchId": ["batch-002"], "metricsType":"attempt_metrics", "forceMerge": false},"parallelization":8,"appName":"ScoreMetricMigrationJob"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val sc: SparkContext = spark.sparkContext
    val res = ScoreMetricMigrationJob.migrateData(spark, jobConfig)
    res.count() should be(1)

    val result = res.select("aggregates", "agg_last_updated", "agg_details").collect().head
    result.get(0).asInstanceOf[Map[String, Int]]("completedCount") should be(1)
    result.get(0).asInstanceOf[Map[String, Int]]("score:do_11307593493010022419") should be(10)
    result.get(0).asInstanceOf[Map[String, Int]]("max_score:do_11307593493010022419") should be(15)
    result.get(0).asInstanceOf[Map[String, Int]]("attempts_count:do_11307593493010022419") should be(1)

    val aggDetail = JSONUtils.deserialize[Map[String, AnyRef]](result.get(2).asInstanceOf[Seq[String]].head)
    aggDetail("max_score") should be(15.0)
    aggDetail("score") should be(10.0)
    aggDetail("type") should be(jobConfig.modelParams.get.get("metricsType").get.toString)
    aggDetail("attempt_id") should be("attempat-001")
    aggDetail("content_id") should be("do_11307593493010022419")
  }

  it should "Should migrate the score metrics data with forcemerge" in {
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.audit.ScoreMetricMigrationJob","modelParams":{"store":"azure","sparkCassandraConnectionHost":"localhost", "fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')", "batchId": [], "metricsType":"attempt_metrics", "forceMerge": true},"parallelization":8,"appName":"ScoreMetricMigrationJob"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val sc: SparkContext = spark.sparkContext
    val res = ScoreMetricMigrationJob.migrateData(spark, jobConfig)

    val result = res.filter(col("context_id") === "cb:batch-001")
      .filter(col("activity_id") === "do_11306040245271756813015")
      .filter(col("user_id") === "user-010")
      .select("aggregates", "agg_last_updated", "agg_details").collect().head
    result.get(0).asInstanceOf[Map[String, Int]]("completedCount") should be(1)
    result.get(0).asInstanceOf[Map[String, Int]]("score:do_11307593493010022418") should be(10)
    result.get(0).asInstanceOf[Map[String, Int]]("max_score:do_11307593493010022418") should be(15)

    // Force merge replaces the existing agg_details
    result.get(2).asInstanceOf[Seq[String]].size should be (1)
    val aggDetail = JSONUtils.deserialize[Map[String, AnyRef]](result.get(2).asInstanceOf[Seq[String]].head)

    aggDetail("max_score") should be(15.0)
    aggDetail("score") should be(10.0)
    aggDetail("type") should be(jobConfig.modelParams.get.get("metricsType").get.toString)
    aggDetail("attempt_id") should be("attempat-002")
    aggDetail("content_id") should be("do_11307593493010022418")
  }
}