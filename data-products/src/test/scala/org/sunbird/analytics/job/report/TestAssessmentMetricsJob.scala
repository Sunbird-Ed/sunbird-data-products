//package org.sunbird.analytics.job.report
//
//import java.time.{ZoneOffset, ZonedDateTime}
//
//import cats.syntax.either._
//import ing.wbaa.druid._
//import ing.wbaa.druid.client.DruidClient
//import io.circe._
//import io.circe.parser._
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.ekstep.analytics.framework.util.{JSONUtils, RestUtil}
//import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, JobConfig}
//import org.scalamock.scalatest.MockFactory
//import org.sunbird.analytics.util.{ESUtil, EmbeddedES, EsResponse}
//import org.sunbird.cloud.storage.BaseStorageService
//import org.sunbird.cloud.storage.conf.AppConf
//
//import scala.collection.mutable.Buffer
//import scala.concurrent.Future
//
//class TestAssessmentMetricsJob extends BaseReportSpec with MockFactory {
//
//  implicit var spark: SparkSession = _
//  implicit var sc: SparkContext = _
//
//  var courseBatchDF: DataFrame = _
//  var userCoursesDF: DataFrame = _
//  var userDF: DataFrame = _
//  var locationDF: DataFrame = _
//  var orgDF: DataFrame = _
//  var userOrgDF: DataFrame = _
//  var externalIdentityDF: DataFrame = _
//  var systemSettingDF: DataFrame = _
//  var assessmentProfileDF: DataFrame = _
//  var reporterMock: ReportGenerator = mock[ReportGenerator]
//  val sunbirdCoursesKeyspace = "sunbird_courses"
//  val sunbirdKeyspace = "sunbird"
//  val esIndexName = "cbatch-assessent-report"
//
//  override def beforeAll(): Unit = {
//    super.beforeAll()
//    spark = getSparkSession();
//    sc = spark.sparkContext
//
//    /*
//     * Data created with 31 active batch from batchid = 1000 - 1031
//     * */
//    courseBatchDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/assessment-metrics-updater/courseBatchTable.csv")
//      .cache()
//
//    externalIdentityDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/assessment-metrics-updater/usr_external_identity.csv")
//      .cache()
//
//    assessmentProfileDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/assessment-metrics-updater/assessment.csv")
//      .cache()
//
//
//    /*
//     * Data created with 35 participants mapped to only batch from 1001 - 1010 (10), so report
//     * should be created for these 10 batch (1001 - 1010) and 34 participants (1 user is not active in the course)
//     * and along with 5 existing users from 31-35 has been subscribed to another batch 1003-1007 also
//     * */
//    userCoursesDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/assessment-metrics-updater/userCoursesTable.csv")
//      .cache()
//
//    /*
//     * This has users 30 from user001 - user030
//     * */
//    userDF = spark
//      .read
//      .json("src/test/resources/assessment-metrics-updater/userTable.json")
//      .cache()
//
//    /*
//     * This has 30 unique location
//     * */
//    locationDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/assessment-metrics-updater/locationTable.csv")
//      .cache()
//
//    /*
//     * There are 8 organisation added to the data, which can be mapped to `rootOrgId` in user table
//     * and `organisationId` in userOrg table
//     * */
//    orgDF = spark
//      .read
//      .json("src/test/resources/assessment-metrics-updater/orgTable.json")
//      .cache()
//
//    /*
//     * Each user is mapped to organisation table from any of 8 organisation
//     * */
//    userOrgDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/assessment-metrics-updater/userOrgtable.csv")
//      .cache()
//
//    systemSettingDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/course-metrics-updater/systemSettingTable.csv")
//      .cache()
//
//    EmbeddedES.loadData("compositesearch", "cs", Buffer(
//      """{"contentType":"SelfAssess","name":"My content 1","identifier":"do_112835335135993856149"}""",
//      """{"contentType":"SelfAssess","name":"My content 2","identifier":"do_112835336280596480151"}""",
//      """{"contentType":"SelfAssess","name":"My content 3","identifier":"do_112832394979106816112"}""",
//      """{"contentType":"Resource","name":"My content 4","identifier":"do_112832394979106816114"}"""
//    ))
//  }
//
//  "AssessmentMetricsJob" should "define all the configurations" in {
//    assert(AppConf.getConfig("assessment.metrics.bestscore.report").isEmpty === false)
//    assert(AppConf.getConfig("assessment.metrics.content.index").isEmpty === false)
//    assert(AppConf.getConfig("assessment.metrics.cassandra.input.consistency").isEmpty === false)
//    assert(AppConf.getConfig("assessment.metrics.cloud.objectKey").isEmpty === false)
//    assert(AppConf.getConfig("cloud.container.reports").isEmpty === false)
//    assert(AppConf.getConfig("assessment.metrics.temp.dir").isEmpty === false)
//    assert(AppConf.getConfig("course.upload.reports.enabled").isEmpty === false)
//    assert(AppConf.getConfig("course.es.index.enabled").isEmpty === false)
//  }
//
//  it should "Ensure for the user `user030` should have all proper records values" in {
//    implicit val mockFc = mock[FrameworkContext];
//    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJob","modelParams":{"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
//    val config = JSONUtils.deserialize[JobConfig](strConfig)
//    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.modelParams.get("druidConfig")))
//    //mocking for DruidDataFetcher
//    import scala.concurrent.ExecutionContext.Implicits.global
//    val json: String =
//      """
//        |{
//        |    "identifier": "do_1125559882615357441175",
//        |    "channel": "01250894314817126443"
//        |  }
//      """.stripMargin
//
//    val doc: Json = parse(json).getOrElse(Json.Null);
//    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc));
//    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)
//
//    implicit val mockDruidConfig = DruidConfig.DefaultConfig
//    val mockDruidClient = mock[DruidClient]
//    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
//    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
//      .returning(courseBatchDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
//      .returning(userCoursesDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
//      .returning(userDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
//      .returning(userOrgDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
//      .returning(orgDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
//      .returning(locationDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
//      .returning(externalIdentityDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
//      .returning(assessmentProfileDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "system_settings", "keyspace" -> sunbirdKeyspace))
//      .anyNumberOfTimes()
//      .returning(systemSettingDF)
//
//    val reportDF = AssessmentMetricsJob
//      .prepareReport(spark, reporterMock.loadData,druidConfig)
//      .cache()
//    val denormedDF = AssessmentMetricsJob.denormAssessment(reportDF)
//    denormedDF.createOrReplaceTempView("report_df")
//    val content1_DF = spark.sql("select * from report_df where userid ='user030' and content_name = 'My content 1' ")
//
//    val username = content1_DF.select("username").collect().map(_ (0)).toList
//    assert(username(0) === "Karishma Kapoor")
//
//    val total_score_value = content1_DF.select("total_sum_score").collect().map(_ (0)).toList
//    assert(total_score_value(0) === "50%")
//
//    val courseid_value = content1_DF.select("courseid").collect().map(_ (0)).toList
//    assert(courseid_value(0) === "do_1125559882615357441175")
//
//    val batchid_value = content1_DF.select("batchid").collect().map(_ (0)).toList
//    assert(batchid_value(0) === "1010")
//
//    val district_name = content1_DF.select("district_name").collect().map(_ (0)).toList
//    assert(district_name(0) === "GULBARGA")
//
//    val org_name = content1_DF.select("orgname_resolved").collect().map(_ (0)).toList
//    assert(org_name(0) === "SACRED HEART(B)PS,TIRUVARANGAM")
//  }
//
//  it should "Sort and get the best score" in {
//    val df = spark.createDataFrame(Seq(
//      ("do_112835335135993856149", "A3", "user030", "do_1125559882615357441175", "1010", "1971-09-22 02:10:53.444+0000", "2019-09-04 09:59:51.000+0000", "10", "5", "2019-09-06 09:59:51.000+0000", "50%", ""),
//      ("do_112835335135993856149", "A3", "user030", "do_1125559882615357441175", "1010", "1971-09-22 02:10:53.444+0000", "2019-09-05 09:59:51.000+0000", "12", "4", "2019-09-06 09:59:51.000+0000", "33%", "")
//    )).toDF("content_id", "attempt_id", "user_id", "course_id", "batch_id", "created_on", "last_attempted_on", "total_max_score", "total_score", "updated_on", "grand_total", "question")
//    val bestScoreDF = AssessmentMetricsJob.getAssessmentData(df);
//    val bestScore = bestScoreDF.select("total_score").collect().map(_ (0)).toList
//    assert(bestScore(0) === "5")
//  }
//
//  it should "Ensure CSV Report Should have all proper columns names" in {
//    implicit val mockFc = mock[FrameworkContext]
//    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJob","modelParams":{"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
//    val config = JSONUtils.deserialize[JobConfig](strConfig)
//    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.modelParams.get("druidConfig")))
//    //mocking for DruidDataFetcher
//    import scala.concurrent.ExecutionContext.Implicits.global
//    val json: String =
//      """
//        |{
//        |    "identifier": "do_1125559882615357441175",
//        |    "channel": "apekx"
//        |  }
//      """.stripMargin
//
//    val doc: Json = parse(json).getOrElse(Json.Null);
//    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc));
//    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)
//
//    implicit val mockDruidConfig = DruidConfig.DefaultConfig
//    val mockDruidClient = mock[DruidClient]
//    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
//    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
//      .returning(courseBatchDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
//      .returning(userCoursesDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
//      .returning(userDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
//      .returning(userOrgDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
//      .returning(orgDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
//      .returning(locationDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
//      .returning(externalIdentityDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
//      .returning(assessmentProfileDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "system_settings", "keyspace" -> sunbirdKeyspace))
//      .anyNumberOfTimes()
//      .returning(systemSettingDF)
//
//    val reportDF = AssessmentMetricsJob
//      .prepareReport(spark, reporterMock.loadData, druidConfig)
//      .cache()
//    val denormedDF = AssessmentMetricsJob.denormAssessment(reportDF)
//    val finalReport = AssessmentMetricsJob.transposeDF(denormedDF)
//    val column_names = finalReport.columns
//    assert(column_names.contains("courseid") === true)
//    assert(column_names.contains("userid") === true)
//    assert(column_names.contains("batchid") === true)
//  }
//
//  it should "generate reports" in {
//    implicit val mockFc = mock[FrameworkContext];
//    val mockStorageService = mock[BaseStorageService]
//    (mockFc.getStorageService(_: String, _: String, _: String)).expects(*, *, *).returns(mockStorageService).anyNumberOfTimes();
//    (mockStorageService.upload _).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
//    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()
//    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJob","modelParams":{"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
//    val config = JSONUtils.deserialize[JobConfig](strConfig)
//    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.modelParams.get("druidConfig")))
//    //mocking for DruidDataFetcher
//    import scala.concurrent.ExecutionContext.Implicits.global
//    val json: String =
//      """
//        |{
//        |    "identifier": "do_1125559882615357441175",
//        |    "channel": "apekx"
//        |  }
//      """.stripMargin
//
//    val doc: Json = parse(json).getOrElse(Json.Null);
//    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc));
//    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)
//
//    implicit val mockDruidConfig = DruidConfig.DefaultConfig
//    val mockDruidClient = mock[DruidClient]
//    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
//    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
//      .returning(courseBatchDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
//      .returning(userCoursesDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
//      .returning(userDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
//      .returning(userOrgDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
//      .returning(orgDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
//      .returning(locationDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
//      .returning(externalIdentityDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
//      .returning(assessmentProfileDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "system_settings", "keyspace" -> sunbirdKeyspace))
//      .anyNumberOfTimes()
//      .returning(systemSettingDF)
//
//    val reportDF = AssessmentMetricsJob
//      .prepareReport(spark, reporterMock.loadData, druidConfig)
//      .cache()
//
//    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
//    val denormedDF = AssessmentMetricsJob.denormAssessment(reportDF)
//    // TODO: Check save is called or not
//    AssessmentMetricsJob.saveReport(denormedDF, tempDir)
//  }
//
//  it should "fetch the only self assess content names from the elastic search" in {
//    val contentESIndex = AppConf.getConfig("assessment.metrics.content.index")
//    assert(contentESIndex.isEmpty === false)
//    val contentList = List("do_112835335135993856149", "do_112835336280596480151", "do_112832394979106816114")
//    val contentDF = ESUtil.getAssessmentNames(spark, contentList, AppConf.getConfig("assessment.metrics.content.index"), AppConf.getConfig("assessment.metrics.supported.contenttype"))
//    contentDF.createOrReplaceTempView("content_metadata")
//    val content_id_df = spark.sql("select * from content_metadata where identifier ='do_112835335135993856149'")
//    val content_name = content_id_df.select("name").collect().map(_ (0)).toList
//    val content_id = content_id_df.select("identifier").collect().map(_ (0)).toList
//    assert(content_name(0) === "My content 1")
//    assert(content_id(0) === "do_112835335135993856149")
//    assert(contentDF.count() === 2) // Length should be 2, Since 2 contents are self assess and 1 content is resource
//  }
//
//  it should "generate reports for the best score" in {
//    implicit val mockFc = mock[FrameworkContext];
//    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJob","modelParams":{"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
//    val config = JSONUtils.deserialize[JobConfig](strConfig)
//    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.modelParams.get("druidConfig")))
//    //mocking for DruidDataFetcher
//    import scala.concurrent.ExecutionContext.Implicits.global
//    val json: String =
//      """
//        |{
//        |    "identifier": "do_1125559882615357441175",
//        |    "channel": "apekx"
//        |  }
//      """.stripMargin
//
//    val doc: Json = parse(json).getOrElse(Json.Null);
//    val results = List(DruidResult.apply(Some(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC)), doc));
//    val druidResponse = DruidResponseTimeseriesImpl.apply(results, QueryType.GroupBy)
//
//    implicit val mockDruidConfig = DruidConfig.DefaultConfig
//    val mockDruidClient = mock[DruidClient]
//    (mockDruidClient.doQuery[DruidResponse](_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
//    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()
//    assessmentProfileDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/assessment-metrics-updater/assessment_best_score.csv")
//      .cache()
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
//      .returning(courseBatchDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
//      .returning(userCoursesDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
//      .returning(userDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
//      .returning(userOrgDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
//      .returning(orgDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
//      .returning(locationDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
//      .returning(externalIdentityDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
//      .returning(assessmentProfileDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "system_settings", "keyspace" -> sunbirdKeyspace))
//      .anyNumberOfTimes()
//      .returning(systemSettingDF)
//
//    val reportDF = AssessmentMetricsJob
//      .prepareReport(spark, reporterMock.loadData,druidConfig)
//      .cache()
//
//    reportDF.createOrReplaceTempView("best_attempt")
//    val df = spark.sql("select * from best_attempt where batchid ='1010' and courseid='do_1125559882615357441175' and content_id='do_112835335135993856149' and userid ='user030'")
//    val best_attempt_score = df.select("total_score").collect().map(_ (0)).toList
//    assert(best_attempt_score.head === "50")
//  }
//
//  it should "Able to create a elastic search index" in {
//    //TODO: Sonar cloud testcase failing.. Need to debug
//    val esResponse: EsResponse = ESUtil.createIndex(AssessmentMetricsJob.getIndexName, "");
//    //assert(esResponse.acknowledged === true)
//  }
//
//  it should "Able to add index to alias" in {
//    ESUtil.createIndex(esIndexName, "");
//    ESUtil.removeIndexFromAlias(List(esIndexName), "cbatch-assessment")
//    val esResponse: EsResponse = ESUtil.addIndexToAlias(esIndexName, "cbatch-assessment");
//    assert(esResponse.acknowledged === true)
//  }
//
//
//  it should "Remove all index from the alias" in {
//    val esResponse: EsResponse = ESUtil.removeAllIndexFromAlias("cbatch-assessment");
//    assert(esResponse.acknowledged === true)
//  }
//
//  it should "not throw any error, Should create alias and index to save data into es" in {
//    val df = spark.createDataFrame(Seq(
//      ("user010", "Manju", "do_534985557934", "batch1", "10", "****@gmail.com", "*****75643","21","Karnataka", "Tumkur", "Orname", "", "NVPHS", "20", "Math", ""),
//      ("user110", "Manoj", "do_534985557934", "batch2", "13", "****@gmail.com", "*****75643", "21","Karnataka","Tumkur", "Orname", "", "NVPHS", "20", "Math", "")
//    )).toDF("userid", "username", "courseid", "batchid", "grand_total", "maskedemail", "maskedphone","schoolUDISE_resolved","state_name", "district_name", "orgname_resolved", "externalid_resolved", "schoolname_resolved", "total_sum_score", "content_name", "reportUrl")
//    try {
//      val indexName = AssessmentMetricsJob.getIndexName
//      AssessmentMetricsJob.saveToElastic(indexName, df, df);
//      val requestURL = ESUtil.elasticSearchURL + "/" + indexName + "/_count?q=courseId:do_534985557934"
//      val response = RestUtil.get[String](requestURL)
//      assert(response !== null)
//    } catch {
//      case ex: Exception => assert(ex === null)
//    }
//  }
//
//}