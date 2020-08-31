package org.sunbird.analytics.job.report

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.ekstep.analytics.framework.util.JSONUtils
import org.scalamock.scalatest.MockFactory

class TestQuestionResponseReport extends BaseReportSpec with MockFactory {
  var spark: SparkSession = _
  val reporterMock = mock[BaseCourseReport]
  val sunbirdCoursesKeyspace = "sunbird_courses"

  var assessmentProfileDF: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();

    assessmentProfileDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/question-report/assessment_agg_data.csv")
      .cache()
  }

  "TestQuestionResponseReport" should "generate the csv with all the required fields" in {
    implicit val mockFc = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.QuestionResponseReport","modelParams":{"batchFilters":["TPD"],"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"127.0.0.0","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'","sparkRedisConnectionHost":"'$sparkRedisConnectionHost'","sparkUserDbRedisIndex":"4","contentFilters":{"request":{"filters":{"framework":"TPD"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel"]}},"reportPath":"question-reports/"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Question Response Dashboard Metrics","deviceMapping":false}"""
    val config = JSONUtils.deserialize[JobConfig](strConfig)

    val outputLocation = "/tmp/question-metrics"
    val outputDir = "question-reports"
    val storageConfig = StorageConfig("local", "", outputLocation)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", new StructType())
      .anyNumberOfTimes()
      .returning(assessmentProfileDF)

    QuestionResponseReport.prepareReport(spark, storageConfig, reporterMock.loadData, config, List())
  }
  }
