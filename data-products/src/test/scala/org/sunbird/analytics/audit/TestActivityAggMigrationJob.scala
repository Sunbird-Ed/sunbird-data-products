package org.sunbird.analytics.audit

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.job.report.{BaseReportSpec, BaseReportsJob}
import org.sunbird.analytics.util.EmbeddedCassandra

class TestActivityAggMigrationJob extends BaseReportSpec with MockFactory {
  implicit var spark: SparkSession = _

  var activityAggDF: DataFrame = _
  var assessmentAggDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdCoursesKeyspace = "sunbird_courses"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    EmbeddedCassandra.loadData("src/test/resources/activity-agg-migration-job/data.cql")
  }

  it should "Should migrate the activity agg table data" in {
    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.audit.ActivityAggMigrationJob","modelParams":{"store":"azure","sparkCassandraConnectionHost":"localhost", "fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"Activity Migration Job"}""".stripMargin
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](strConfig)
    implicit val sc: SparkContext = spark.sparkContext
    val res = ActivityAggMigrationJob.migrateData(spark)
    res.count() should be(5)
    println("ress" + res.show(false))
    val result = res.filter(col("context_id") === "cb:batch-001")
      .filter(col("activity_id") === "do_11306040245271756813015")
      .filter(col("user_id") === "user-008")
      .select("agg", "agg_last_updated").collect()
    result.head.get(0).asInstanceOf[Map[String, Int]]("completedCount") should be(0)
    result.head.get(0).asInstanceOf[Map[String, Int]]("score:do_112876961957437440179") should be(10)
    result.head.get(0).asInstanceOf[Map[String, Int]]("max_score:do_112876961957437440179") should be(10)
    ActivityAggMigrationJob.updatedTable(res, ActivityAggMigrationJob.userActivityAggDBSettings)
  }

}