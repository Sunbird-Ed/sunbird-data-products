//package org.sunbird.analytics.job.report
//
//import org.apache.spark.sql.functions.{udf, _}
//import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
//import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
//import org.ekstep.analytics.framework.util.JSONUtils
//import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
//import org.scalamock.scalatest.MockFactory
//import org.sunbird.analytics.job.report.CollectionSummaryJob.saveToBlob
//import org.sunbird.analytics.util.UserData
//
//import scala.collection.mutable
//
//
///** *
// * *************************************** START OF TEST INPUT ****************************************
// * Report1 = (batch = (0130320389509939204), course = (do_112636984058314752121), enrolledUsers(c7ef3848-bbdb-4219-8344-817d5b8103fa, user021), channel (b00bc992ef25f1a9a8d63291e20efc8d)
// * Report2 = (batch = (0130293763489873929), course = (do_1130293726460805121168), enrolledUsers(f3dd58a4-a56f-4c1d-95cf-3231927a28e9, user026), channel (013016492159606784174))
// * Report3 = (batch = (01303150537737011211), course = (do_1130314965721088001129), enrolledUsers(user026, user025), channel (b00bc992ef25f1a9a8d63291e20efc8d))
// * Report4 = (batch = (0130271096968396800), course = (do_1130264512015646721166), enrolledUsers(user027), channel (013016492159606784174))
// *
// * channel_org_map = (channel = b00bc992ef25f1a9a8d63291e20efc8d, orgId = 0126391644091351040 , orgName = "MPPS BAYYARAM"), (channel = 013016492159606784174, orgId = 0125302909498654720 , orgName = MPPS SIMHACHALNAGAR)
// * user_org_map = (
// * (orgName = "MPPS BAYYARAM", userId = (c7ef3848-bbdb-4219-8344-817d5b8103fa, user021, f3dd58a4-a56f-4c1d-95cf-3231927a28e9)),
// * orgName = "MPPS SIMHACHALNAGAR", userId = (user026, user025, user027)),
// * )
// * *************************************** END OF TEST INPUT ******************************************
// */
//
//
//class TestCollectionSummaryJob extends BaseReportSpec with MockFactory {
//
//
//  var spark: SparkSession = _
//
//  var courseBatchDF: DataFrame = _
//  var userEnrolments: DataFrame = _
//  var userDF: DataFrame = _
//  var organisationDF: DataFrame = _
//  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
//  val sunbirdCoursesKeyspace = "sunbird_courses"
//  val sunbirdKeyspace = "sunbird"
//  val esIndexName = "composite"
//
//  override def beforeAll(): Unit = {
//    super.beforeAll()
//    spark = getSparkSession();
//    courseBatchDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/collection-summary/course_batch_data.csv")
//      .cache()
//
//    userEnrolments = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/collection-summary/user_courses_data.csv")
//      .cache()
//
//    userDF = spark.read.json("src/test/resources/collection-summary/user_data.json")
//      .cache()
//
//  }
//
//  val convertMethod = udf((value: mutable.WrappedArray[String]) => {
//    if (null != value && value.nonEmpty)
//      value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
//    else null
//  }, new ArrayType(MapType(StringType, StringType), true))
//
//  it should "generate the report for all the batches" in intercept[Exception] {
//
//    (reporterMock.fetchData _)
//      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace, "cluster" -> "LMSCluster"), "org.apache.spark.sql.cassandra", new StructType())
//      .returning(courseBatchDF.withColumn("cert_templates", lit(null).cast(MapType(StringType, MapType(StringType, StringType)))))
//
//    (reporterMock.fetchData _)
//      .expects(spark, Map("table" -> "user_enrolments", "keyspace" -> sunbirdCoursesKeyspace, "cluster" -> "LMSCluster"), "org.apache.spark.sql.cassandra", new StructType())
//      .returning(userEnrolments.withColumn("certificates", convertMethod(split(userEnrolments.col("certificates"), ",").cast("array<string>")))
//        .withColumn("issued_certificates", convertMethod(split(userEnrolments.col("issued_certificates"), ",").cast("array<string>")))
//      )
//      .anyNumberOfTimes()
//
//    val schema = Encoders.product[UserData].schema
//    (reporterMock.fetchData _)
//      .expects(spark, Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid"), "org.apache.spark.sql.redis", schema)
//      .anyNumberOfTimes()
//      .returning(userDF)
//
//
//    implicit val mockFc: FrameworkContext = mock[FrameworkContext]
//    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CollectionSummaryJob","modelParams":{"searchFilter":{"request":{"filters":{"status":["Live"],"contentType":"Course","keywords":["Training"]},"fields":["identifier","name","organisation","channel"],"limit":10000}},"coloumns":["Published by","Batch id","Collection id","Collection name","Batch start date","Batch end date","State","Total enrolments By State","Total completion By State"],"store":"azure","sparkElasticsearchConnectionHost":"{{ sunbird_es_host }}","sparkRedisConnectionHost":"{{ metadata2_redis_host }}","sparkUserDbRedisIndex":"12","sparkCassandraConnectionHost":"{{ core_cassandra_host }}","fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')"},"parallelization":8,"appName":"Collection Summary Report"}""".stripMargin
//    implicit val jobConfig = JSONUtils.deserialize[JobConfig](strConfig)
//    val data = CollectionSummaryJob.prepareReport(spark, reporterMock.fetchData)
//    saveToBlob(data, jobConfig)
//  }
//
//}