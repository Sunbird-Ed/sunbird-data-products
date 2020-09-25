package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, concat, count, lit, split}
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, Level, StorageConfig}
import org.ekstep.analytics.model.ReportConfig
import org.ekstep.analytics.util.Constants
import org.sunbird.analytics.model.report.{TenantInfo, TenantResponse}
import org.sunbird.analytics.util.TextBookUtils
import org.sunbird.cloud.storage.conf.AppConf

case class ContentHierarchy(identifier: String, hierarchy: String)
case class TextbookReportResult(identifier: String, l1identifier: String,board: String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String, totalChapters: String)
case class ContentReportResult(identifier: String, l1identifier: String, contentType: String)
case class TextbookHierarchy(channel: String, board: String, identifier: String, medium: Object, gradeLevel: List[String], subject: Object,
                             name: String, status: String, contentType: Option[String], leafNodesCount: Int, lastUpdatedOn: String,
                             depth: Int, createdOn: String, children: Option[List[TextbookHierarchy]], index: Int, parent: String)
case class FinalReport(identifier: String,l1identifier: String,board: String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String, totalChapters: String, slug:String)
case class TextbookResponse(l1identifier:String,board: String, medium: String, grade: String, subject: String, name: String, chapters: String, channel: String)

object VDNMetricsJob extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.sunbird.analytics.job.VDNMetricsJob"
  val sunbirdHierarchyStore: String = AppConf.getConfig("course.metrics.cassandra.sunbirdHierarchyStore")

  // add for output blob values
  // $COVERAGE-OFF$ Disabling scoverage for main method
  def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    JobLogger.log("Started execution - Textbook Report Job",None, Level.INFO)

    implicit val sparkContext: SparkContext = getReportingSparkContext(JSONUtils.deserialize[JobConfig](config))
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()

    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val sparkConf = sparkContext.getConf
      .set("es.write.operation", "upsert")
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    generateVDNReport(spark, config)
  }

  // $COVERAGE-ON$ Enabling scoverage for all other functions
  def generateVDNReport(spark: SparkSession, config: String)(implicit sc: SparkContext,fc: FrameworkContext): Unit = {
    val conf = JSONUtils.deserialize[Map[String,AnyRef]](config)
    val textbooks = TextBookUtils.getTextBooks(conf, RestUtil)
    JobLogger.log(s"Fetched textbooks from druid ${textbooks.length}",None, Level.INFO)

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
        val report = textbookData.map(f => TextbookReportResult(textbook.identifier,f.l1identifier,textbook.board,textbook.medium,textbook.gradeLevel,textbook.subject,textbook.name,f.chapters,textbook.channel,totalChapters))
        textbookReportData = report.reverse ++ textbookReportData
        contentReportData = contentData ++ contentReportData
      }
      (textbookReportData,contentReportData)
    })

    val textbookReports = sc.parallelize(textbookReportData).map(f=>(f.channel,f))
    val tenantInfo = getTenantInfo(RestUtil).map(f=>(f.id,f))
    val textbookResult = TextbookReportResult("","","","","","","","","","")

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val report = textbookReports.fullOuterJoin(tenantInfo).map(f=> FinalReport(f._2._1.getOrElse(textbookResult).identifier,f._2._1.getOrElse(textbookResult).l1identifier,
      f._2._1.getOrElse(textbookResult).board,f._2._1.getOrElse(textbookResult).medium,f._2._1.getOrElse(textbookResult).grade,
      f._2._1.getOrElse(textbookResult).subject,f._2._1.getOrElse(textbookResult).name,f._2._1.getOrElse(textbookResult).chapters,
      f._2._1.getOrElse(textbookResult).channel,f._2._1.getOrElse(textbookResult).totalChapters,
      f._2._2.getOrElse(TenantInfo("","Unknown")).slug)).toDF()
    val contentdf = contentReportData.toDF()
    val contentChapter = contentdf.groupBy("identifier","l1identifier")
      .pivot(concat(lit("Number of "), col("contentType"))).agg(count("l1identifier"))
    val contentTb = contentdf.groupBy("identifier")
      .pivot(concat(lit("Number of "), col("contentType"))).agg(count("identifier"))

    val storageConfig = getStorageConfig("reports", "")

    val textbookReport = report.join(contentTb, Seq("identifier"),"inner")
      .drop("identifier","channel","id","chapters","l1identifier")
      .orderBy('medium,split(split('grade,",")(0)," ")(1).cast("int"),'subject,'name)
    saveReportToBlob(textbookReport, config, storageConfig, "TextbookLevel")

    val chapterReport = report.join(contentChapter, Seq("identifier","l1identifier"),"inner")
      .drop("identifier","l1identifier","channel","id","totalChapters")
      .orderBy('medium,split(split('grade,",")(0)," ")(1).cast("int"),'subject,'name,'chapters)
    JobLogger.log(s"VDNMetricsJob: extracted chapter and textbook reports", None, INFO)
    saveReportToBlob(chapterReport, config, storageConfig, "ChapterLevel")

  }

  def generateReport(data: List[TextbookHierarchy], prevData: List[TextbookResponse], newData: List[TextbookHierarchy],textbookInfo: TextbookHierarchy, contentInfo: List[ContentReportResult], chapterInfo: List[String]): (List[TextbookResponse],List[ContentReportResult],String) = {
    var textbookReport = prevData
    var contentData = contentInfo
    var l1identifier = chapterInfo(0)
    var totalChapters = chapterInfo(1)
    var textbook = List[TextbookHierarchy]()

    data.map(units=> {
      val children = units.children
      if(units.depth==1) {
        textbook = units :: newData
        l1identifier = units.identifier
        val grade = TextBookUtils.getString(textbookInfo.gradeLevel)
        val report = TextbookResponse(l1identifier,textbookInfo.board,TextBookUtils.getString(textbookInfo.medium),grade,TextBookUtils.getString(textbookInfo.subject),textbookInfo.name,units.name,textbookInfo.channel)
        totalChapters = (totalChapters.toInt+1).toString
        textbookReport = report :: textbookReport
      }

      if(units.depth!=0 && units.contentType.getOrElse("").nonEmpty) {
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

  def getTenantInfo(restUtil: HTTPClient)(implicit sc: SparkContext): RDD[TenantInfo] = {
    val url = Constants.ORG_SEARCH_URL

    val tenantRequest = s"""{
                           |    "params": { },
                           |    "request":{
                           |        "filters": {"isRootOrg":"true"},
                           |        "offset": 0,
                           |        "limit": 10000,
                           |        "fields": ["id", "channel", "slug", "orgName"]
                           |    }
                           |}""".stripMargin
    sc.parallelize(restUtil.post[TenantResponse](url, tenantRequest).result.response.content)
  }

  def saveReportToBlob(data: DataFrame, config: String, storageConfig: StorageConfig, reportName: String): Unit = {
    val configMap = JSONUtils.deserialize[Map[String,AnyRef]](config)
    val reportconfigMap = configMap("modelParams").asInstanceOf[Map[String, AnyRef]]("reportConfig")
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(reportconfigMap))

    val fieldsList = data.columns
    val filteredDf = data.select(fieldsList.head, fieldsList.tail: _*)
    val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
    val renamedDf = filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*)
      .withColumn("reportName",lit(reportName))

    reportConfig.output.map(format => {
      renamedDf.saveToBlobStore(storageConfig, format.`type`, "",
        Option(Map("header" -> "true")), Option(List("slug","reportName")))
    })

  }

}
