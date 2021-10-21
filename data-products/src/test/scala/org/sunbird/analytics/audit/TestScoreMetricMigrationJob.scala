package org.sunbird.analytics.audit

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.job.report.{BaseReportSpec, BaseReportsJob}
import org.sunbird.analytics.util.EmbeddedCassandra

class TestScoreMetricMigrationJob extends BaseReportSpec with MockFactory {
  implicit var spark: SparkSession = _

  var activityAggDF: DataFrame = _
  var assessmentAggDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdCoursesKeyspace = "sunbird_courses"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
  }

  override def beforeEach(): Unit = {
    EmbeddedCassandra.loadData("src/test/resources/score-metrics-migration-job/data.cql")
  }

  override def afterEach(): Unit = {
    EmbeddedCassandra.close()
  }

  it should "Should migrate the score metrics data" in {
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.audit.ScoreMetricMigrationJob","modelParams":{"store":"azure","sparkCassandraConnectionHost":"localhost", "fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')", "metrics_type":"attempt_metrics"},"parallelization":8,"appName":"ScoreMetricMigrationJob"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val sc: SparkContext = spark.sparkContext
    val res = ScoreMetricMigrationJob.migrateData(spark, jobConfig)
    res.count() should be(6)
    println("ress" + res.show(false))
    val result = res.filter(col("context_id") === "cb:batch-001")
      .filter(col("activity_id") === "do_11306040245271756813015")
      .filter(col("user_id") === "user-008")
      .select("agg", "agg_last_updated", "agg_details").collect()
    result.head.get(0).asInstanceOf[Map[String, Int]]("completedCount") should be(0)
    result.head.get(0).asInstanceOf[Map[String, Int]]("score:do_112876961957437440179") should be(10)
    result.head.get(0).asInstanceOf[Map[String, Int]]("max_score:do_112876961957437440179") should be(10)
    result.head.get(2).asInstanceOf[Seq[String]].head should be("""{"max_score":10.0,"score":10.0,"type":"attempt_metrics","attempt_id":"attempat-001","content_id":"do_112876961957437440110"}""")
    result.head.get(2).asInstanceOf[Seq[String]](1) should be("""{"max_score":10.0,"score":10.0,"type":"attempt_metrics","attempt_id":"attempat-001","content_id":"do_112876961957437440179"}""")

    val result2 = res.filter(col("context_id") === "cb:batch-001")
      .filter(col("activity_id") === "do_11306040245271756813015")
      .filter(col("user_id") === "user-010")
      .select("agg_details").collect()

    result2.head.get(0).asInstanceOf[Seq[String]].head should be("""{"max_score":15.0,"score":15.0,"type":"attempt_metrics","attempt_id":"attempat-001","content_id":"do_11307593493010022418"}""")
    result2.head.get(0).asInstanceOf[Seq[String]](1) should be("""{"max_score":15.0,"score":10.0,"type":"attempt_metrics","attempt_id":"attempat-002","content_id":"do_11307593493010022418"}""")
    ScoreMetricMigrationJob.updatedTable(res, ScoreMetricMigrationJob.userActivityAggDBSettings)
  }

  it should "Should migrate the score metrics data for specific batch" in {
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.audit.ScoreMetricMigrationJob","modelParams":{"store":"azure","sparkCassandraConnectionHost":"localhost", "fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')", "batchId": ["batch-002"], "metrics_type":"attempt_metrics"},"parallelization":8,"appName":"ScoreMetricMigrationJob"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val sc: SparkContext = spark.sparkContext
    val res = ScoreMetricMigrationJob.migrateData(spark, jobConfig)
    res.count() should be(1)
    println("ress2" + res.show(false))

    val result = res.select("agg", "agg_last_updated", "agg_details").collect()
    result.head.get(0).asInstanceOf[Map[String, Int]]("completedCount") should be(1)
    result.head.get(0).asInstanceOf[Map[String, Int]]("score:do_11307593493010022419") should be(10)
    result.head.get(0).asInstanceOf[Map[String, Int]]("max_score:do_11307593493010022419") should be(15)
    result.head.get(2).asInstanceOf[Seq[String]].head should be("""{"max_score":15.0,"score":10.0,"type":"attempt_metrics","attempt_id":"attempat-001","content_id":"do_11307593493010022419"}""")
  }
}