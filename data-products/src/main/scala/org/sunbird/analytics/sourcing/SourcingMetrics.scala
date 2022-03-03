package org.sunbird.analytics.sourcing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, concat, count, lit, split}
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, HTTPClient, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, IJob, JobConfig, Level, StorageConfig}
import org.ekstep.analytics.model.ReportConfig
import org.ekstep.analytics.util.Constants
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.analytics.model.report.{TenantInfo, TenantResponse, TextbookData}
import org.sunbird.analytics.util.TextBookUtils
import org.sunbird.cloud.storage.conf.AppConf

case class ContentHierarchy(identifier: String, hierarchy: String)
case class TextbookInfo(channel: String, identifier: String, name: String, createdFor: String, createdOn: String, lastUpdatedOn: String,
                        board: String, medium: String, gradeLevel: String, subject: String, status: String, primaryCategory: String)
case class TextbookReportResult(identifier: String, l1identifier: String, board: String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String, totalChapters: String, primaryCategory: String)
case class ContentReportResult(identifier: String, l1identifier: String, contentType: String)
case class TextbookHierarchy(channel: String, identifier: String, medium: Object, gradeLevel: List[String], subject: Object,
                             name: String, status: String, contentType: Option[String], leafNodesCount: Int, lastUpdatedOn: String,
                             depth: Int, createdOn: String, children: Option[List[TextbookHierarchy]], index: Int, parent: String)
case class FinalReport(identifier: String,l1identifier: String,board: String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String, totalChapters: String, slug:String)
case class TextbookResponse(l1identifier:String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String)

object SourcingMetrics extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.sunbird.analytics.job.SourcingMetrics"
  val jobName: String = "Sourcing Metrics Job"
  val sunbirdHierarchyStore: String = AppConf.getConfig("course.metrics.cassandra.sunbirdHierarchyStore")
  val unitTypes = List("courseunit","textbookunit","collection")
  val collectionTypes = List("course","textbook", "content playlist", "question paper")

  // $COVERAGE-OFF$ Disabling scoverage for main method
  def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    JobLogger.init(jobName)
    JobLogger.start(s"Started execution - $jobName",Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    implicit val spark = openSparkSession(jobConfig)

    try {
      val res = CommonUtil.time(execute());
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1, "chapterReportCount" -> res._2.getOrElse("chapterReportCount",0), "textbookReportCount" -> res._2.getOrElse("textbookReportCount",0))))
    } catch {
      case ex: Exception =>
        JobLogger.log(ex.getMessage, None, Level.ERROR);
        JobLogger.end(s"$jobName execution failed", "FAILED", Option(Map("model" -> jobName, "statusMsg" -> ex.getMessage)));
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  // $COVERAGE-ON$ Enabling scoverage for all other functions
  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Map[String, Long] = {
    implicit val sc = spark.sparkContext
    import spark.implicits._
    val textbooks = getTextBooks(config.modelParams.get)
    JobLogger.log(s"Fetched textbooks from druid ${textbooks.length}",None, Level.INFO)

    val textbookReportData = getTextbookInfo(textbooks)
    val textbookReports = textbookReportData._1.toDF()
    val tenantInfo = getTenantInfo(RestUtil).toDF()
    JobLogger.log(s"$jobName: Generated report metrics - textbook: ${textbookReportData._1.length}, contents: ${textbookReportData._2.length}", None, INFO)
    val report = textbookReports.join(tenantInfo,
      textbookReports.col("channel") === tenantInfo.col("id"),"left").na.fill("Unknown", Seq("slug"))
    process(textbookReportData._2.toDF(), report)
  }

  def process(contentdf: DataFrame, report: DataFrame)(implicit spark: SparkSession, config: JobConfig): Map[String, Long] = {
    import spark.implicits._
    val contentChapter = contentdf.groupBy("identifier","l1identifier")
      .pivot(concat(lit("Number of "), col("contentType"))).agg(count("l1identifier"))
    val contentTb = contentdf.groupBy("identifier")
      .pivot(concat(lit("Number of "), col("contentType"))).agg(count("identifier"))
    val configMap = JSONUtils.deserialize[Map[String,AnyRef]](JSONUtils.serialize(config.modelParams.get))
    val reportConfig = configMap("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportPath = reportConfig.getOrElse("reportPath","sourcing").asInstanceOf[String]
    val storageConfig = getStorageConfig(config, "")

    val textbookReport = report.join(contentTb, Seq("identifier"),"left")
      .drop("identifier","channel","id","chapters","l1identifier")
      .distinct()
      .orderBy('medium,split(split('grade,",")(0)," ")(1).cast("int"),'subject,'name)
    saveReportToBlob(textbookReport, JSONUtils.serialize(reportConfig), storageConfig, "CollectionLevel", reportPath)

    val chapterReport = report.join(contentChapter, Seq("identifier","l1identifier"),"left")
      .drop("identifier","l1identifier","channel","id","totalChapters")
      .orderBy('medium,split(split('grade,",")(0)," ")(1).cast("int"),'subject,'name,'chapters)
    JobLogger.log(s"$jobName: extracted chapter and textbook reports", None, INFO)
    saveReportToBlob(chapterReport, JSONUtils.serialize(reportConfig), storageConfig, "FolderLevel", reportPath)
    Map("textbookReportCount"->textbookReport.count(),"chapterReportCount"->chapterReport.count())
  }

  def getTextbookInfo(textbooks: List[TextbookInfo])(implicit spark: SparkSession,fc: FrameworkContext): (List[TextbookReportResult],List[ContentReportResult]) = {
    val encoders = Encoders.product[ContentHierarchy]
    var textbookReportData = List[TextbookReportResult]()
    var contentReportData = List[ContentReportResult]()

    val result = textbooks.map(textbook => {
      val textbookHierarchy = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "content_hierarchy", "keyspace" -> sunbirdHierarchyStore)).load()
        .where(col("identifier") === textbook.identifier)
      val count = textbookHierarchy.count()
      if(count > 0) {
        val textbookRdd = textbookHierarchy.as[ContentHierarchy](encoders).first()
        val hierarchy = JSONUtils.deserialize[TextbookHierarchy](textbookRdd.hierarchy)
        val reportMetrics = generateReport(List(hierarchy),List(), List(),hierarchy,List(),List("","0"))
        val textbookData = reportMetrics._1
        val contentData = reportMetrics._2
        val totalChapters = reportMetrics._3
        val report = textbookData.map(f => TextbookReportResult(textbook.identifier,f.l1identifier,textbook.board,textbook.medium,textbook.gradeLevel,textbook.subject,textbook.name,f.chapters,textbook.channel,totalChapters, textbook.primaryCategory))
        textbookReportData = report.reverse ++ textbookReportData
        contentReportData = contentData ++ contentReportData
      }
      (textbookReportData,contentReportData)
    })
    (textbookReportData,contentReportData)
  }

  def generateReport(data: List[TextbookHierarchy], prevData: List[TextbookResponse], newData: List[TextbookHierarchy],textbookInfo: TextbookHierarchy, contentInfo: List[ContentReportResult], chapterInfo: List[String]): (List[TextbookResponse],List[ContentReportResult],String) = {
    var textbookReport = prevData
    var contentData = contentInfo
    var l1identifier = chapterInfo(0)
    var totalChapters = chapterInfo(1)
    var textbook = List[TextbookHierarchy]()

    data.map(units=> {
      val children = units.children
      if(units.depth == 1 && (unitTypes.contains(units.contentType.getOrElse("").toLowerCase) || collectionTypes.contains(units.contentType.getOrElse("").toLowerCase))) {
        textbook = units :: newData
        l1identifier = units.identifier
        val grade = TextBookUtils.getString(textbookInfo.gradeLevel)
        val report = TextbookResponse(l1identifier, TextBookUtils.getString(textbookInfo.medium),grade,TextBookUtils.getString(textbookInfo.subject),textbookInfo.name,units.name,textbookInfo.channel)
        totalChapters = (totalChapters.toInt+1).toString
        textbookReport = report :: textbookReport
      }
      if(units.depth != 0 && units.contentType.getOrElse("").nonEmpty && !unitTypes.contains(units.contentType.getOrElse("").toLowerCase)) {
        contentData = ContentReportResult(textbookInfo.identifier,l1identifier, units.contentType.get) :: contentData
      }
      if(children.isDefined) {
        val textbookReportData = generateReport(children.get, textbookReport, textbook,textbookInfo, contentData,List(l1identifier,totalChapters))
        textbookReport = textbookReportData._1
        contentData = textbookReportData._2
        totalChapters = textbookReportData._3
      }
    })
    (textbookReport,contentData,totalChapters)
  }

  def getTextBooks(config: Map[String, AnyRef])(implicit sc:SparkContext,fc: FrameworkContext): List[TextbookInfo] = {
    val request = JSONUtils.serialize(config.get("druidConfig").get)
    val druidQuery = JSONUtils.deserialize[DruidQueryModel](request)
    val druidResponse = DruidDataFetcher.getDruidData(druidQuery, true)

    val result = druidResponse.map(f => {
      JSONUtils.deserialize[TextbookInfo](f)
    })
    result.collect().toList
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

  def saveReportToBlob(data: DataFrame, reportConf: String, storageConfig: StorageConfig, reportName: String, reportPath: String): Unit = {
    val reportConfig = JSONUtils.deserialize[ReportConfig](reportConf)
    val fieldsList = data.columns
    val filteredDf = data.select(fieldsList.head, fieldsList.tail: _*)
    val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
    val renamedDf = filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*)
      .withColumn("reportName",lit(reportName))
    JobLogger.log(s"$jobName: Saving $reportName to blob", None, INFO)

    reportConfig.output.map(format => {
      renamedDf.saveToBlobStore(storageConfig, format.`type`, reportPath,
        Option(Map("header" -> "true")), Option(List("slug","reportName")))
    })
  }

}
