package org.sunbird.analytics.sourcing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, IJob, JobConfig, JobContext, Level, StorageConfig}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, HTTPClient, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.model.ReportConfig
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.sunbird.analytics.job.report.BaseReportsJob
import org.sunbird.analytics.model.report.{TenantInfo, TenantResponse}

case class ProgramData(program_id: String, name: String, rootorg_id: String, channel: String,
                       status: String, startdate: String, enddate: String)
case class NominationData(program_id: String, Initiated : String, Pending: String,
                          Rejected: String, Approved: String)
case class ContributionResult(result: ContributionResultData, responseCode: String)
case class ContributionResultData(content: List[Contributions], count: Int)
case class Contributions(acceptedContents: List[String],rejectedContents: List[String])
case class TotalContributionResult(result: TotalContributionData, responseCode: String)
case class TotalContributionData(facets: List[TotalContributions], count: Int)
case class TotalContributions(values:List[ContributionData])
case class ContributionData(name:String,count:Int)
case class ProgramVisitors(program_id:String, startdate:String, enddate:String, visitors:String)
case class FunnelResult(program_id:String, reportDate: String, projectName: String, initiatedNominations: String,
                        rejectedNominations: String, pendingNominations: String, acceptedNominations: String,
                        noOfContributors: String, noOfContributions: String, pendingContributions: String,
                        approvedContributions: String, channel: String)
case class VisitorResult(date: String, visitors: String, slug: String, reportName: String)
case class DruidTextbookData(visitors: Int)

object FunnelReport extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.sunbird.analytics.job.report.FunnelReport"
  val jobName = "Funnel Report Job"
  val db = AppConf.getConfig("postgres.db")
  val url = AppConf.getConfig("postgres.url") + s"$db"
  val connProperties = CommonUtil.getPostgresConnectionProps
  val programTable = "program"
  val nominationTable = "nomination"

  // $COVERAGE-OFF$ Disabling scoverage for main method
  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None): Unit = {
    JobLogger.init("FunnelReport")
    JobLogger.start("Started execution - FunnelReport Job",Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)

    JobContext.parallelization = CommonUtil.getParallelization(jobConfig)
    implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()

    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")

    val sparkConf = sparkContext.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
      .set("spark.sql.caseSensitive", AppConf.getConfig(key = "spark.sql.caseSensitive"))
    implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    try {
      val res = CommonUtil.time(execute());
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "funnelReportCount" -> res._2.getOrElse("funnelReportCount",0))))
    } catch {
      case ex: Exception =>
        JobLogger.log(ex.getMessage, None, Level.ERROR);
        JobLogger.end(s"$jobName execution failed", "FAILED", Option(Map("model" -> jobName, "statusMsg" -> ex.getMessage)));
    } finally {
      frameworkContext.closeContext()
    }
  }

  // $COVERAGE-ON$ Enabling scoverage for all other functions
  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Map[String, Long] = {
    implicit val sc = spark.sparkContext
    import spark.implicits._
    val tenantInfo = getTenantInfo(RestUtil).toDF()
    process(tenantInfo)
  }

  def process(tenantInfo: DataFrame)(implicit spark: SparkSession, config: JobConfig): Map[String, Long] = {
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val programEncoder = Encoders.product[ProgramData]
    val programData = spark.read.jdbc(url, programTable, connProperties).as[ProgramData](programEncoder).rdd
      .map(f => (f.program_id,f))

    val nominationEncoder = Encoders.product[NominationData]
    val nominationData = spark.read.jdbc(url, nominationTable, connProperties)
    val nominations = nominationData.groupBy("program_id")
      .pivot(col("status"), Seq("Initiated","Pending","Rejected","Approved"))
      .agg(count("program_id"))
      .na.fill(0)
    val nominationRdd = nominations
      .as[NominationData](nominationEncoder).rdd
      .map(f => (f.program_id,f))

    val reportDate = DateTimeFormat.forPattern("dd-MM-yyyy").print(DateTime.now())

    val data = programData.leftOuterJoin(nominationRdd).map(f=>(f._2._1,f._2._2.getOrElse(NominationData("","","","",""))))
    var druidData = List[ProgramVisitors]()
    val configMap = JSONUtils.deserialize[Map[String,AnyRef]](JSONUtils.serialize(config))

    val report = data
      .filter(f=> null != f._1.status && (f._1.status.equalsIgnoreCase("Live") || f._1.status.equalsIgnoreCase("Closed") || f._1.status.equalsIgnoreCase("Unlisted"))).collect().toList
      .map(f => {
        val contributionData = getContributionData(f._1.program_id, configMap("modelParams").asInstanceOf[Map[String, AnyRef]])
        druidData = ProgramVisitors(f._1.program_id,f._1.startdate,f._1.enddate, "0") :: druidData
        FunnelResult(f._1.program_id,reportDate,f._1.name,f._2.Initiated,f._2.Rejected,
          f._2.Pending,f._2.Approved,contributionData._1.toString,contributionData._2.toString,contributionData._3.toString,
          contributionData._4.toString,f._1.rootorg_id)
      }).toDF()

    val funnelReport = report.join(tenantInfo,report.col("channel") === tenantInfo.col("id"),"left")
      .drop("channel","id","program_id")
      .persist(StorageLevel.MEMORY_ONLY)

    val storageConfig = getStorageConfig("reports", "")
    saveReportToBlob(funnelReport, configMap, storageConfig, "FunnelReport")

    funnelReport.unpersist(true)
    Map("funnelReportCount"->funnelReport.count())
  }

  def getDruidQuery(query: String, programId: String, interval: String): DruidQueryModel = {
    val mapQuery = JSONUtils.deserialize[Map[String,AnyRef]](query)
    val filters = JSONUtils.deserialize[List[Map[String, AnyRef]]](JSONUtils.serialize(mapQuery("filters")))
    val updatedFilters = filters.map(f => {
      f map {
        case ("value","program_id") => "value" -> programId
        case x => x
      }
    })
    val finalMap = mapQuery.updated("filters",updatedFilters) map {
      case ("intervals","startdate/enddate") => "intervals" -> interval
      case x => x
    }
    JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(finalMap))
  }

  def getContributionRequest(query: String, programId: String): String = {
    val mapQuery = JSONUtils.deserialize[Map[String,AnyRef]](query)
    val request = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(mapQuery("request")))
    val filters = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(request("filters")))
    val updatedFilters = filters map {
      case ("programId","programIdentifier") => "programId" -> programId
      case x => x
    }

    val finalMap = mapQuery.updated("request",request.updated("filters",updatedFilters))
    JSONUtils.serialize(finalMap)
  }

  def getContributionData(programId: String, config: Map[String, AnyRef]): (Int,Int,Int,Int) = {
    val url = Constants.COMPOSITE_SEARCH_URL
    val contributionConfig = JSONUtils.deserialize[Map[String,AnyRef]](JSONUtils.serialize(config("contributionConfig")))

    val contributionRequest = getContributionRequest(JSONUtils.serialize(contributionConfig("contributionRequest")), programId)
    val contributionResponse = RestUtil.post[TotalContributionResult](url,contributionRequest)
    val contributionResponses =if(null != contributionResponse && contributionResponse.responseCode.equalsIgnoreCase("OK") && contributionResponse.result.count>0) {
      contributionResponse.result.facets
    } else List()
    val totalContributors = contributionResponses.filter(p => null!=p.values).flatMap(f=>f.values).length
    val totalContributions = contributionResponses.filter(p => null!=p.values).flatMap(f=> f.values).map(f=>f.count).sum

    val correctionsPendingRequest = getContributionRequest(JSONUtils.serialize(contributionConfig("correctionsPendingRequest")), programId)
    val correctionsPendingResponse = RestUtil.post[TotalContributionResult](url,correctionsPendingRequest)
    val correctionResponses =if(null != correctionsPendingResponse && correctionsPendingResponse.responseCode.equalsIgnoreCase("OK") && correctionsPendingResponse.result.count>0) {
      correctionsPendingResponse.result.facets
    } else List()
    val correctionPending=correctionResponses.filter(p => null!=p.values).flatMap(f=> f.values).map(f=>f.count).sum

    val tbRequest =getContributionRequest(JSONUtils.serialize(contributionConfig("contentRequest")), programId)
    val response = RestUtil.post[ContributionResult](url,tbRequest)

    val contentData = if(null != response && response.responseCode.equalsIgnoreCase("OK") && response.result.count>0) {
      response.result.content
    } else List()
    val acceptedContents = contentData.filter(p => null!=p.acceptedContents).flatMap(f=>f.acceptedContents).length
    val rejectedContents = contentData.filter(p => null!=p.rejectedContents).flatMap(f=>f.rejectedContents).length
    val contents = acceptedContents+rejectedContents
    val pendingContributions = if(totalContributions-contents > 0) totalContributions-contents else 0

    (totalContributors,totalContributions+correctionPending,pendingContributions,acceptedContents)
  }

  def getTenantInfo(restUtil: HTTPClient)(implicit sc: SparkContext): RDD[TenantInfo] = {
    val url = Constants.ORG_SEARCH_URL

    val tenantRequest = s"""{
                           |    "params": { },
                           |    "request":{
                           |        "filters": {"isTenant":"true"},
                           |        "offset": 0,
                           |        "limit": 10000,
                           |        "fields": ["id", "channel", "slug", "orgName"]
                           |    }
                           |}""".stripMargin
    sc.parallelize(restUtil.post[TenantResponse](url, tenantRequest).result.response.content)
  }

  def saveReportToBlob(data: DataFrame, config: Map[String,AnyRef], storageConfig: StorageConfig, reportName: String): Unit = {
    val reportconfigMap = config("modelParams").asInstanceOf[Map[String, AnyRef]]("reportConfig")
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(reportconfigMap))

    val fieldsList = data.columns
    val filteredDf = data.select(fieldsList.head, fieldsList.tail: _*)
    val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
    val renamedDf = filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*)
      .withColumn("reportName",lit(reportName))

    reportConfig.output.map(format => {
      renamedDf.saveToBlobStore(storageConfig, format.`type`, "sourcing",
        Option(Map("header" -> "true")), Option(List("slug","reportName")))
    })

  }

}
