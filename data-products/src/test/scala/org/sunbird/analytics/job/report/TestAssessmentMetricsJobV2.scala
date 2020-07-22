package org.sunbird.analytics.job.report

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.EmbeddedES
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable.Buffer

class TestAssessmentMetricsJobV2 extends BaseReportSpec with MockFactory {

  implicit var spark: SparkSession = _

  var courseBatchDF: DataFrame = _
  var userCoursesDF: DataFrame = _
  var assessmentProfileDF: DataFrame = _
  var userInfoDF: DataFrame = _
  var reporterMock: ReportGeneratorV2 = mock[ReportGeneratorV2]
  val sunbirdCoursesKeyspace = "sunbird_courses"
  val esIndexName = "cbatch-assessent-report"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();

    /*
     * Data created with 31 active batch from batchid = 1000 - 1031
     * */
    courseBatchDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updaterv2/courseBatchTable.csv")
      .cache()

    userInfoDF = spark.read.json("src/test/resources/course-metrics-updaterv2/user_data.json").cache()

    assessmentProfileDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updaterv2/assessment.csv")
      .cache()

    AssessmentMetricsJobV2.loadData(spark, Map("table" -> "user", "keyspace" -> "sunbird"),"org.apache.spark.sql.cassandra")
    /*
     * Data created with 35 participants mapped to only batch from 1001 - 1010 (10), so report
     * should be created for these 10 batch (1001 - 1010) and 34 participants (1 user is not active in the course)
     * and along with 5 existing users from 31-35 has been subscribed to another batch 1003-1007 also
     * */
    userCoursesDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updaterv2/userCoursesTable.csv")
      .cache()

    EmbeddedES.loadData("compositesearch", "cs", Buffer(
      """{"contentType":"SelfAssess","name":"My content 1","identifier":"do_112835335135993856149"}""",
      """{"contentType":"SelfAssess","name":"My content 2","identifier":"do_112835336280596480151"}""",
      """{"contentType":"SelfAssess","name":"My content 3","identifier":"do_112832394979106816112"}""",
      """{"contentType":"Resource","name":"My content 4","identifier":"do_112832394979106816114"}"""
    ))
  }

  "AssessmentMetricsJobV2" should "define all the configurations" in {
    assert(AppConf.getConfig("assessment.metrics.bestscore.report").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.content.index").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.cassandra.input.consistency").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.cloud.objectKey").isEmpty === false)
    assert(AppConf.getConfig("cloud.container.reports").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.temp.dir").isEmpty === false)
    assert(AppConf.getConfig("course.upload.reports.enabled").isEmpty === false)
    assert(AppConf.getConfig("course.es.index.enabled").isEmpty === false)
  }

  it should "Sort and get the best score" in {
    val df = spark.createDataFrame(Seq(
      ("do_112835335135993856149", "A3", "user030", "do_1125559882615357441175", "1010", "1971-09-22 02:10:53.444+0000", "2019-09-04 09:59:51.000+0000", "10", "5", "2019-09-06 09:59:51.000+0000", "50%", ""),
      ("do_112835335135993856149", "A3", "user030", "do_1125559882615357441175", "1010", "1971-09-22 02:10:53.444+0000", "2019-09-05 09:59:51.000+0000", "12", "4", "2019-09-06 09:59:51.000+0000", "33%", "")
    )).toDF("content_id", "attempt_id", "user_id", "course_id", "batch_id", "created_on", "last_attempted_on", "total_max_score", "total_score", "updated_on", "grand_total", "question")
    val bestScoreDF = AssessmentMetricsJobV2.getAssessmentData(df);
    val bestScore = bestScoreDF.select("total_score").collect().map(_ (0)).toList
    assert(bestScore(0) === "5")
  }

  it should "Ensure CSV Report Should have all proper columns names" in {
    implicit val mockFc = mock[FrameworkContext]
    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJob","modelParams":{"batchFilters":["TPD"],"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
    val config = JSONUtils.deserialize[JobConfig](strConfig)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("keys.pattern" -> "*","infer.schema" -> "true"),"org.apache.spark.sql.redis")
      .anyNumberOfTimes()
      .returning(userInfoDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJobV2
      .prepareReport(spark, reporterMock.loadData, "NCF", List("1006","1005","1015","1016"), config)
      .cache()
    val denormedDF = AssessmentMetricsJobV2.denormAssessment(reportDF)
    val finalReport = AssessmentMetricsJobV2.transposeDF(denormedDF)
    val column_names = finalReport.columns
    assert(column_names.contains("courseid") === true)
    assert(column_names.contains("userid") === true)
    assert(column_names.contains("batchid") === true)

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val report = reportDF.withColumn("content_name", lit("Content-1"))
      .withColumn("total_sum_score",lit("90"))
      .withColumn("grand_total", lit("100"))
    AssessmentMetricsJobV2.saveToAzure(report,"","1006",report, config)
    AssessmentMetricsJobV2.saveReport(report, tempDir, "true", config)

    val reportWithoutBatchDetails = reportDF.na.replace("batchid",Map("1006"->""))
    AssessmentMetricsJobV2.saveReport(reportWithoutBatchDetails, tempDir, "true", config)
  }

  it should "generate reports" in {
    implicit val mockFc = mock[FrameworkContext];
    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String, _: String, _: String)).expects(*, *, *).returns(mockStorageService).anyNumberOfTimes();
    (mockStorageService.upload _).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()
    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJob","modelParams":{"batchFilters":["NCF"],"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
    val config = JSONUtils.deserialize[JobConfig](strConfig)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("keys.pattern" -> "*","infer.schema" -> "true"),"org.apache.spark.sql.redis")
      .anyNumberOfTimes()
      .returning(userInfoDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJobV2
      .prepareReport(spark, reporterMock.loadData, "TPD", List(), config)
      .cache()

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val denormedDF = AssessmentMetricsJobV2.denormAssessment(reportDF)
    // TODO: Check save is called or not
    AssessmentMetricsJobV2.saveReport(denormedDF, tempDir, "false", config)
  }

  it should "Ensure CSV Report Should have all proper columns names for NISHTHA" in {
    implicit val mockFc = mock[FrameworkContext]
    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJob","modelParams":{"reportId": "NISHTHA-reports", "batchFilters":["TPD"],"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
    val config = JSONUtils.deserialize[JobConfig](strConfig)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("keys.pattern" -> "*","infer.schema" -> "true"),"org.apache.spark.sql.redis")
      .anyNumberOfTimes()
      .returning(userInfoDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJobV2
      .prepareReport(spark, reporterMock.loadData, "NCF", List("1006","1005","1015","1016"), config)
      .cache()
    val denormedDF = AssessmentMetricsJobV2.denormAssessment(reportDF)
    val finalReport = AssessmentMetricsJobV2.transposeDF(denormedDF)
    val column_names = finalReport.columns
    assert(column_names.contains("courseid") === true)
    assert(column_names.contains("userid") === true)
    assert(column_names.contains("batchid") === true)

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val report = reportDF.withColumn("content_name", lit("Content-1"))
      .withColumn("total_sum_score",lit("90"))
      .withColumn("grand_total", lit("100"))
    AssessmentMetricsJobV2.saveToAzure(report,"","1006",report, config)
    AssessmentMetricsJobV2.saveReport(report, tempDir, "true", config)

    val reportWithoutBatchDetails = reportDF.na.replace("batchid",Map("1006"->""))
    AssessmentMetricsJobV2.saveReport(reportWithoutBatchDetails, tempDir, "true", config)
  }

  it should "return an empty list if no assessment names found for given content" in {
    val result = AssessmentMetricsJobV2.getAssessmentNames(spark, List("do_1126458775024025601296","do_1126458775024025"), "Resource")
    result.collect().length should be(0)
  }

}