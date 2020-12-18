//package org.sunbird.analytics.job.report
//
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
//import org.ekstep.analytics.framework.util.JSONUtils
//import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
//import org.scalamock.scalatest.MockFactory
//import org.sunbird.analytics.util.{CourseUtils, EmbeddedES, UserData}
//import org.sunbird.cloud.storage.BaseStorageService
//
//import scala.collection.mutable.Buffer
//
//class TestAssessmentMetricsJobV2 extends BaseReportSpec with MockFactory {
//
//  var spark: SparkSession = _
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
//  var userInfoDF: DataFrame = _
//  var reporterMock: ReportGeneratorV2 = mock[ReportGeneratorV2]
//  val sunbirdCoursesKeyspace = "sunbird_courses"
//  val sunbirdKeyspace = "sunbird"
//  val esIndexName = "cbatch-assessent-report"
//
//  override def beforeAll(): Unit = {
//    super.beforeAll()
//    spark = getSparkSession();
//
//    /*
//     * Data created with 31 active batch from batchid = 1000 - 1031
//     * */
//    courseBatchDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/assessment-metrics-updaterv2/courseBatchTable.csv")
//      .cache()
//
//    externalIdentityDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/assessment-metrics-updaterv2/usr_external_identity.csv")
//      .cache()
//
//    userInfoDF = spark.read.json("src/test/resources/course-metrics-updaterv2/user_data.json").cache()
//
//    assessmentProfileDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/assessment-metrics-updaterv2/assessment.csv")
//      .cache()
//
//    AssessmentMetricsJobV2.loadData(spark, Map("table" -> "user", "keyspace" -> "sunbird"),"org.apache.spark.sql.cassandra", new StructType())
//    /*
//     * Data created with 35 participants mapped to only batch from 1001 - 1010 (10), so report
//     * should be created for these 10 batch (1001 - 1010) and 34 participants (1 user is not active in the course)
//     * and along with 5 existing users from 31-35 has been subscribed to another batch 1003-1007 also
//     * */
//    userCoursesDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/assessment-metrics-updaterv2/userCoursesTable.csv")
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
//  "AssessmentMetricsJobV2" should "Sort and get the best score" in {
//    val df = spark.createDataFrame(Seq(
//      ("do_112835335135993856149", "A3", "user030", "do_1125559882615357441175", "1010", "1971-09-22 02:10:53.444+0000", "2019-09-04 09:59:51.000+0000", "10", "5", "2019-09-06 09:59:51.000+0000", "50%", ""),
//      ("do_112835335135993856149", "A3", "user030", "do_1125559882615357441175", "1010", "1971-09-22 02:10:53.444+0000", "2019-09-05 09:59:51.000+0000", "12", "4", "2019-09-06 09:59:51.000+0000", "33%", ""),
//      ("do_112835335135993856149", "A3", "user030", "do_1125559882615357441175", "1010", "1971-09-22 02:10:53.444+0000", "2019-09-05 09:59:51.000+0000", "12", "4", "2019-09-06 09:59:51.000+0000", "33%", "")
//    )).toDF("content_id", "attempt_id", "user_id", "course_id", "batch_id", "created_on", "last_attempted_on", "total_max_score", "total_score", "updated_on", "grand_total", "question")
//    val bestScoreDF = AssessmentMetricsJobV2.getAssessmentData(df);
//    val bestScore = bestScoreDF.select("total_score").collect().map(_ (0)).toList
//    assert(bestScore(0) === "5")
//  }
//
//  it should "ensure all data" in {
//    implicit val mockFc = mock[FrameworkContext];
//    val mockStorageService = mock[BaseStorageService]
//    (mockFc.getStorageService(_: String, _: String, _: String)).expects(*, *, *).returns(mockStorageService).anyNumberOfTimes();
//    (mockStorageService.upload _).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
//    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()
//
//    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJob","modelParams":{"batchFilters":["NCF"],"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
//    val config = JSONUtils.deserialize[JobConfig](strConfig)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
//      .returning(courseBatchDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
//      .returning(userCoursesDF)
//      .anyNumberOfTimes()
//
//    val schema = Encoders.product[UserData].schema
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user","infer.schema" -> "true", "key.column"-> "userid"),"org.apache.spark.sql.redis", schema)
//      .anyNumberOfTimes()
//      .returning(userInfoDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
//      .anyNumberOfTimes()
//      .returning(assessmentProfileDF)
//
//    AssessmentMetricsJobV2.prepareReport(spark, reporterMock.loadData, config, List())
//  }
//
//  it should "generate report per batch" in {
//    implicit val mockFc = mock[FrameworkContext];
//    val mockStorageService = mock[BaseStorageService]
//    (mockFc.getStorageService(_: String, _: String, _: String)).expects(*, *, *).returns(mockStorageService).anyNumberOfTimes();
//    (mockStorageService.upload _).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
//    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()
//
//    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJob","modelParams":{"batchFilters":["NCF"],"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
//    val config = JSONUtils.deserialize[JobConfig](strConfig)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
//      .returning(courseBatchDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
//      .returning(userCoursesDF)
//      .anyNumberOfTimes()
//
//    val schema = Encoders.product[UserData].schema
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user","infer.schema" -> "true", "key.column"-> "userid"),"org.apache.spark.sql.redis", schema)
//      .anyNumberOfTimes()
//      .returning(userInfoDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
//      .anyNumberOfTimes()
//      .returning(assessmentProfileDF)
//
//    AssessmentMetricsJobV2.prepareReport(spark, reporterMock.loadData, config, List("1006"))
//  }
//
//  it should "return an empty list if no assessment names found for given content" in {
//    val result = AssessmentMetricsJobV2.getAssessmentNames(spark, List("do_1126458775024025601296","do_1126458775024025"), "Resource")
//    result.collect().length should be(0)
//  }
//
//  it should "process the filtered batches for assessment" in {
//    implicit val mockFc = mock[FrameworkContext]
//    implicit val sc = spark
//    val conf = """{"search": {"type": "none"},"model": "org.sunbird.analytics.job.report.AssessmentMetricsJob","modelParams": {"batchFilters": ["TPD"],"fromDate": "$(date --date yesterday '+%Y-%m-%d')","toDate": "$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost": "127.0.0.0","sparkElasticsearchConnectionHost": "'$sunbirdPlatformElasticsearchHost'","sparkRedisConnectionHost": "'$sparkRedisConnectionHost'","sparkUserDbRedisIndex": "4","contentFilters": {"request": {"filters": {"framework": "TPD"},"sort_by": {"createdOn": "desc"},"limit": 10000,"fields": ["framework", "identifier", "name", "channel"]}},"reportPath": "assessment-reports/"},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Assessment Dashboard Metrics","deviceMapping": false}""".stripMargin
//    val jobConf = JSONUtils.deserialize[JobConfig](conf)
//    val mockStorageService = mock[BaseStorageService]
//
//    (mockFc.getStorageService(_: String, _: String, _: String)).expects(*, *, *).returns(mockStorageService).anyNumberOfTimes();
//    (mockStorageService.upload _).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
//    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
//      .returning(courseBatchDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
//      .returning(userCoursesDF)
//      .anyNumberOfTimes()
//
//    val schema = Encoders.product[UserData].schema
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "user","infer.schema" -> "true", "key.column"-> "userid"),"org.apache.spark.sql.redis", schema)
//      .anyNumberOfTimes()
//      .returning(userInfoDF)
//
//    (reporterMock.loadData _)
//      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
//      .anyNumberOfTimes()
//      .returning(assessmentProfileDF)
//
//    AssessmentMetricsJobV2.prepareReport(spark, reporterMock.loadData, jobConf, List())
//    val contents = CourseUtils.filterContents(spark, """{"request": {"filters": {"channel": "0898765434567891"},"sort_by": {"createdOn": "desc"},"limit": 10000,"fields": ["framework", "identifier", "name", "channel"]}}""".stripMargin)
//    val assessmentDF = AssessmentMetricsJobV2.getAssessmentProfileDF(reporterMock.loadData)
//    val batch = CourseBatch("1006","","2018-12-01","in.ekstep")
//    val reportDf = AssessmentMetricsJobV2.getReportDF(batch, userInfoDF, assessmentDF, false)
//    val contentIds: List[String] = reportDf.select(col("content_id")).distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]]
//
//    val denormDF = AssessmentMetricsJobV2.denormAssessment(reportDf, contentIds)
//    AssessmentMetricsJobV2.saveReport(denormDF, "/tmp/assessment-report","true","1006","assessment-reports/")
//
//    reportDf.count() should be(2)
//  }
//
//}