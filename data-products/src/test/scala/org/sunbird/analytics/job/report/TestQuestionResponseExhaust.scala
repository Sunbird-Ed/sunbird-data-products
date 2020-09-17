package org.sunbird.analytics.job.report

import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.scalamock.scalatest.MockFactory

class TestQuestionResponseExhaust extends BaseReportSpec with MockFactory {
  var spark: SparkSession = _
  val reporterMock = mock[BaseCourseReport]
  val sunbirdCoursesKeyspace = "sunbird_courses"

  var assessmentProfileDF: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();

//    assessmentProfileDF = spark
//      .read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .load("src/test/resources/question-report/assessment_agg_data.csv")
//      .cache()

    implicit val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._

    assessmentProfileDF = List(
      AssessmentAggData("do_213045554296676352193", "0130207040554762244", "04b12c81-67e3-4a1a-8c5d-222d2019ae2d", "do_2130257157776097281995", "8017dc1150022b9370c2effb5be681b2", "2020-06-18 13:26:14.583000+0000", "12.33/13.0", "2020-06-18 12:51:21.324000+0000", List(QuestionData("do_2129194942597447681595", 1, 0, "mcq","choose one",List(Map("2" -> """{"text" -> "Quantity}""")), List(), "choose one", "0.0"), QuestionData("do_2129194957310361601598", 1, 0, "mcq", "choose correct one", List(), List(Map("1" -> """{"text":"Pen Paper Test"}""", "2" -> """{"text":"Online Test"}""")), "", "0.0")), "13", "12.33", "2020-06-18 13:26:14.631000+0000"),
      AssessmentAggData("do_213045554296676352193", "0130207040554762244", "8454cb21-3ce9-4e30-85b5-fade097880d8", "do_2130257157776097281995", "85b3283314c2680581f9447c0b792dc2a3", "2020-06-19 13:26:14.583000+0000", "2.33/3.0", "2020-06-19 12:51:21.324000+0000", List(QuestionData("do_2129194942597447681595", 1, 1, "mcq","Which three subject areas are mainly assessed by PISA?",List(), List(Map("1" -> """{"text":"Science, Fine Arts and Environments Studies\n"}""", "2" -> """{"text":"Reading Literacy, Mathematical Literacy and Scientific Abilities\n"}""")), "choose one", "0.0"), QuestionData("do_2129194957310361601598", 1, 0, "mcq", "choose correct one", List(), List(Map("1" -> """{"text":"Pen Paper Test"}""", "2" -> """{"text":"Online Test"}""")), "", "0.0")), "13", "12.33", "2020-06-18 13:26:14.631000+0000"),
      AssessmentAggData("do_11269863149590937614", "0130759417337364480", "8454cb21-3ce9-4e30-85b5-fade097880d8", "do_1126980913198940161169", "85b32814c2680581f9447c0b792dc2a3", "2020-06-19 13:26:14.583000+0000", "0/3", "2020-06-19 12:51:21.324000+0000", List(QuestionData("do_2129194942597447681595", 1, 1, "mcq","Which three subject areas are mainly assessed by PISA?",List(), List(Map("1" -> """{"text":"Science, Fine Arts and Environments Studies\n"}""", "2" -> """{"text":"Reading Literacy, Mathematical Literacy and Scientific Abilities\n"}""")), "", "0.0")), "3", "0", "2020-06-18 13:26:14.631000+0000")
    ).toDF()
  }

  "TestQuestionResponseExhaust" should "generate the csv with all the required fields" in {
    implicit val mockFc = mock[FrameworkContext]
    val strConfig = """{"search":{"type":"none"},"model":"org.sunbird.analytics.job.report.QuestionResponseExhaust","modelParams":{"batchFilters":["TPD"], "esConfig": {"request":{"filters":{"objectType":["Content"],"contentType":["Course"],"identifier":[]},"fields":["identifier","name","batches.batchId","batches.name"],"limit":10000}},"fromDate":"$(date --date yesterday '+%Y-%m-%d')","toDate":"$(date --date yesterday '+%Y-%m-%d')","sparkCassandraConnectionHost":"127.0.0.0","sparkElasticsearchConnectionHost":"'$sunbirdPlatformElasticsearchHost'","sparkRedisConnectionHost":"'$sparkRedisConnectionHost'","sparkUserDbRedisIndex":"4","contentFilters":{"request":{"filters":{"framework":"TPD"},"sort_by":{"createdOn":"desc"},"limit":10000,"fields":["framework","identifier","name","channel"]}},"reportPath":"question-exhaust-reports/"},"output":[{"to":"console","params":{"printEvent":false}}],"parallelization":8,"appName":"Question Response Dashboard Metrics","deviceMapping":false}"""
    val config = JSONUtils.deserialize[JobConfig](strConfig)

    val outputLocation = "/tmp/question-metrics"
    val outputDir = "question-reports"
    val storageConfig = StorageConfig("local", "", outputLocation)
    val schema = Encoders.product[AssessmentAggData].schema

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra", schema)
      .anyNumberOfTimes()
      .returning(assessmentProfileDF)

    QuestionResponseExhaust.prepareReport(spark, storageConfig, reporterMock.loadData, config, List())
  }
  }
