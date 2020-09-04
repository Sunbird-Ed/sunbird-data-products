package org.sunbird.analytics.job.report

import org.apache.spark.sql.functions.{split, udf}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.UserData
import org.sunbird.cloud.storage.BaseStorageService

import scala.collection.mutable

case class UserAgg(activity_type: String, activity_id: String, user_id: String, context_id: String, agg: Map[String, Int], agg_last_updated: String)

case class ContentHierarchy(identifier: String, hierarchy: String)

class TestCourseReport extends BaseReportSpec with MockFactory with BaseReportsJob {
  var spark: SparkSession = _
  var courseBatchDF: DataFrame = _
  var userCoursesDF: DataFrame = _
  var assessmentProfileDF: DataFrame = _
  var userDF: DataFrame = _
  var userActivityAgg: DataFrame = _
  var userEnrolmentDF: DataFrame = _
  var contentHierarchyDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  override val sunbirdCoursesKeyspace = "sunbird_courses"
  override val sunbirdHierarchyStore = "dev_hierarchy_store"
  override val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    implicit val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._

    courseBatchDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updaterv2/course_batch_data.csv").cache()

    userCoursesDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/course-metrics-updaterv2/user_courses_data.csv").cache()

    userDF = spark.read.json("src/test/resources/course-metrics-updaterv2/user_data.json").cache()

    userActivityAgg = List(
      UserAgg("Course", "do_1130314965721088001129", "c7ef3848-bbdb-4219-8344-817d5b8103fa", "cb:0130561083009187841", Map("completedCount" -> 1), "{'completedCount': '2020-07-21 08:30:48.855000+0000'}"),
      UserAgg("Course", "do_13456760076615812", "f3dd58a4-a56f-4c1d-95cf-3231927a28e9", "cb:0130561083009187841", Map("completedCount" -> 1), "{'completedCount': '2020-07-21 08:30:48.855000+0000'}"),
      UserAgg("Course", "do_1125105431453532161282", "be28a4-a56f-4c1d-95cf-3231927a28e9", "cb:0130561083009187841", Map("completedCount" -> 5), "{'completedCount': '2020-07-21 08:30:48.855000+0000'}")).toDF()
      .select("user_id", "activity_id", "agg", "context_id")

    contentHierarchyDF = List(ContentHierarchy("do_1130314965721088001129", """{"mimeType": "application/vnd.ekstep.content-collection","children": [{"children": [{"mimeType": "application/vnd.ekstep.content-collection","contentType": "CourseUnit","identifier": "do_1125105431453532161282","visibility": "Parent","name": "Untitled sub Course Unit 1.2"}],"mimeType": "collection","contentType": "Course","visibility": "Default","identifier": "do_1125105431453532161282","leafNodesCount": 3}, {"contentType": "Course","identifier": "do_1125105431453532161282","name": "Untitled Course Unit 2"}],"contentType": "Course","identifier": "do_1130314965721088001129","visibility": "Default","leafNodesCount": 9}"""),
      ContentHierarchy("do_13456760076615812", """{"mimeType": "application/vnd.ekstep.content-collection","children": [{"children": [{"mimeType": "application/vnd.ekstep.content-collection","contentType": "CourseUnit","identifier": "do_1125105431453532161282","visibility": "Parent","name": "Untitled sub Course Unit 1.2"}],"mimeType": "application/vnd.ekstep.content-collection","contentType": "CourseUnit","identifier": "do_1125105431453532161282"}, {"contentType": "CourseUnit","identifier": "do_1125105431453532161282","name": "Untitled Course Unit 2"}],"contentType": "Course","identifier": "do_13456760076615812","visibility": "Default","leafNodesCount": 4}""")).toDF()

    assessmentProfileDF = spark.read.format("com.databricks.spark.csv").option("header", "true")
      .load("src/test/resources/assessment-metrics-updaterv2/assessment.csv").cache()
  }

  "CourseReportJob" should "Generate report for both assessmenUserAggt and course" in {
    implicit val sc = spark
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJob","modelParams":{"batchFilters":["NCF"],"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""

    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_: String, _: String, _: String)).expects(*, *, *).returns(mockStorageService).anyNumberOfTimes()
    (mockStorageService.upload _).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes()

    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()

    val convertMethod = udf((value: mutable.WrappedArray[String]) => {
      if (null != value && value.nonEmpty)
        value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
      else null
    }, new ArrayType(MapType(StringType, StringType), true))

    val alteredUserCourseDf = userCoursesDF.withColumn("certificates", convertMethod(split(userCoursesDF.col("certificates"), ",").cast("array<string>")))

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace), "org.apache.spark.sql.cassandra", Some(new StructType()), Some(Seq("courseid", "batchid", "enddate", "startdate")))
      .returning(courseBatchDF)


    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace), "org.apache.spark.sql.cassandra", Some(new StructType()), Some(Seq("course_id", "batch_id", "user_id", "content_id", "total_max_score", "total_score", "grand_total")))
      .anyNumberOfTimes()
      .returning(assessmentProfileDF)

    val schema = Encoders.product[UserData].schema

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "user_activity_agg", "keyspace" -> sunbirdCoursesKeyspace), "org.apache.spark.sql.cassandra", Some(new StructType()), Some(Seq("user_id", "activity_id", "agg", "context_id")))
      .anyNumberOfTimes()
      .returning(userActivityAgg)

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore), "org.apache.spark.sql.cassandra", Some(new StructType()), Some(Seq("identifier", "hierarchy")))
      .anyNumberOfTimes()
      .returning(contentHierarchyDF)

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid"), "org.apache.spark.sql.redis", Some(schema), Some(Seq("firstname", "lastname", "userid", "state", "district")))
      .anyNumberOfTimes()
      .returning(userDF)

    (reporterMock.fetchData _)
      .expects(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace), "org.apache.spark.sql.cassandra", Some(new StructType()), Some(Seq("batchid", "userid", "courseid", "active", "certificates", "enrolleddate", "completedon")))
      .anyNumberOfTimes()
      .returning(alteredUserCourseDf)

    CourseReport.generateReports(null, JSONUtils.deserialize[Map[String, AnyRef]](strConfig), fetchData = reporterMock.fetchData)
    println("CourseMetricsReport===")

  }

}
