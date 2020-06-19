package org.sunbird.analytics.job.report

import java.time.{ZoneOffset, ZonedDateTime}

import cats.syntax.either._
import ing.wbaa.druid._
import ing.wbaa.druid.client.DruidClient
import io.circe._
import io.circe.parser._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.util.{HadoopFileUtil, JSONUtils}
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, JobConfig, StorageConfig}
import org.scalamock.scalatest.MockFactory
import org.sunbird.analytics.util.EmbeddedES

import scala.collection.mutable
import scala.concurrent.Future

case class ESOutput(name: String, id: String, userId: String, completedOn: String, maskedEmail: String, maskedPhone: String, rootOrgName: String, subOrgName: String, startDate: String, courseId: String, lastUpdatedOn: String, batchId: String, completedPercent: String, districtName: String, blockName: String, externalId: String, subOrgUDISECode: String,StateName: String, enrolledOn: String, certificateStatus: String)
case class ESOutputCBatch(id: String, reportUpdatedOn: String, completedCount: String, participantCount: String)

class TestCourseMetricsJob extends BaseReportSpec with MockFactory {
  var spark: SparkSession = _
  var courseBatchDF: DataFrame = _
  var userCoursesDF: DataFrame = _
  var userDF: DataFrame = _
  var locationDF: DataFrame = _
  var orgDF: DataFrame = _
  var userOrgDF: DataFrame = _
  var externalIdentityDF: DataFrame = _
  var systemSettingDF: DataFrame = _
  var reporterMock: ReportGenerator = mock[ReportGenerator]
  val sunbirdCoursesKeyspace = "sunbird_courses"
  val sunbirdKeyspace = "sunbird"

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
      .load("src/test/resources/course-metrics-updater/courseBatchTable.csv")
      .cache()

    externalIdentityDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/usr_external_identity.csv")
      .cache()

    /*
     * Data created with 35 participants mapped to only batch from 1001 - 1010 (10), so report
     * should be created for these 10 batch (1001 - 1010) and 34 participants (1 user is not active in the course)
     * and along with 5 existing users from 31-35 has been subscribed to another batch 1003-1007 also
     * */
    userCoursesDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/userCoursesTable.csv")
      .cache()

    /*
     * This has users 30 from user001 - user030
     * */
    userDF = spark
      .read
      .json("src/test/resources/course-metrics-updater/userTable.json")
      .cache()

    /*
     * This has 30 unique location
     * */
    locationDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/locationTable.csv")
      .cache()

    /*
     * There are 8 organisation added to the data, which can be mapped to `rootOrgId` in user table
     * and `organisationId` in userOrg table
     * */
    orgDF = spark
      .read
      .json("src/test/resources/course-metrics-updater/orgTable.json")
      .cache()

    /*
     * Each user is mapped to organisation table from any of 8 organisation
     * */
    userOrgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/userOrgtable.csv")
      .cache()

    systemSettingDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/course-metrics-updater/systemSettingTable.csv")
      .cache()
  }
  
  "TestUpdateCourseMetrics" should "generate reports for 10 batches and validate all scenarios" in {

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF);


    val convertMethod = udf((value: mutable.WrappedArray[String]) => {
      if(null != value && value.length > 0)
        value.toList.map(str => JSONUtils.deserialize(str)(manifest[Map[String, String]])).toArray
      else null
    }, new ArrayType(MapType(StringType, StringType), true))
    
    val alteredUserCourseDf = userCoursesDF.withColumn("certificates", convertMethod(split(userCoursesDF.col("certificates"), ",").cast("array<string>")) )
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .anyNumberOfTimes()
      .returning(alteredUserCourseDf)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "system_settings", "keyspace" -> sunbirdKeyspace))
      .anyNumberOfTimes()
      .returning(systemSettingDF)

    val storageConfig = StorageConfig("local", "", "src/test/resources/course-metrics")

    implicit val mockFc = mock[FrameworkContext];
    val strConfig= """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.CourseMetricsJob","modelParams":{"druidConfig":{"queryType":"groupBy","dataSource":"content-model-snapshot","intervals":"LastDay","granularity":"all","aggregations":[{"name":"count","type":"count","fieldName":"count"}],"dimensions":[{"fieldName":"identifier","aliasName":"identifier"},{"fieldName":"channel","aliasName":"channel"}],"filters":[{"type":"equals","dimension":"contentType","value":"Course"}],"descending":"false"},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"'$sunbirdPlatformCassandraHost'","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Course Dashboard Metrics","deviceMapping":false}"""
    val config = JSONUtils.deserialize[JobConfig](strConfig)
    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.modelParams.get("druidConfig")))
    //mocking for DruidDataFetcher
    import scala.concurrent.ExecutionContext.Implicits.global
    val json: String =
      """
        |{
        |    "identifier": "do_1127102863240151041169",
        |    "channel": "apekx"
        |  }
      """.stripMargin

    val doc: Json = parse(json).getOrElse(Json.Null);
    val results = List(DruidResult.apply(ZonedDateTime.of(2020, 1, 23, 17, 10, 3, 0, ZoneOffset.UTC), doc));
    val druidResponse = DruidResponse.apply(results, QueryType.GroupBy)

    implicit val mockDruidConfig = DruidConfig.DefaultConfig
    val mockDruidClient = mock[DruidClient]
    (mockDruidClient.doQuery(_: DruidQuery)(_: DruidConfig)).expects(*, mockDruidConfig).returns(Future(druidResponse)).anyNumberOfTimes()
    (mockFc.getDruidClient _).expects().returns(mockDruidClient).anyNumberOfTimes()

    CourseMetricsJob.prepareReport(spark, storageConfig, reporterMock.loadData,config)

    (new HadoopFileUtil()).delete(spark.sparkContext.hadoopConfiguration, "src/test/resources/course-metrics")
  }

}