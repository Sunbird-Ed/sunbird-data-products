package org.ekstep.analytics.model.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.model.ReportConfig
import org.sunbird.analytics.util.CourseUtils.loadData
import org.sunbird.analytics.util.{CourseUtils, TextbookUtils, UnirestUtil}
import org.sunbird.cloud.storage.conf.AppConf

//Tenant Information from cassandra
case class TenantInfo(id: String, slug: String) extends AlgoInput

//Textbook information from composite-search
case class ContentResult(channel: String, identifier: String, board: String, gradeLevel: List[String], medium: Object, subject: Object, status: String, creator: String, lastPublishedOn: String, lastSubmittedOn: String, createdFor: List[String], createdOn: String, contentType: String, mimeType: String, resourceType: Object, pkgVersion: Integer)

//Aggregated Report for each tenant
case class AggregatedReport(board: String, medium: String, gradeLevel: String, subject: String, resourceType: String, live: Integer, review: Integer, draft: Integer, unlisted: Integer, application_ecml: Integer, video_youtube: Integer, video_mp4: Integer, application_pdf: Integer, application_html: Integer, identifier: String, slug: String, reportName: String = "Aggregated Report")

//Live Report for each tenant
case class LiveReport(board: String, medium: String, gradeLevel: String, identifier: String, resourceType: String, createdOn: String, pkgVersion: Integer, creator: String, lastPublishedOn: String, slug: String, reportName: String = "Live Report")

//Non-live report for each tenant
case class NonLiveStatusReport(board: String, medium: String, gradeLevel: String, identifier: String, resourceType: String, status: String, pendingInCurrentStatus: String, creator: String, createdOn: String, slug: String, reportName: String = "Non Live Report")

case class FinalOutput(identifier: String, aggregatedReport: Option[AggregatedReport], liveReport: Option[LiveReport], nonLiveStatusReport: Option[NonLiveStatusReport]) extends AlgoOutput with Output

object TextbookProgressModel extends IBatchModelTemplate[Empty, TenantInfo, FinalOutput, FinalOutput] with Serializable {

  implicit val className: String = "org.ekstep.analytics.model.TextbookProgressModel"

  override def name: String = "TextbookProgressModel"

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[TenantInfo] = {
    CommonUtil.setStorageConf(config.getOrElse("store", "local").toString, config.get("accountKey").asInstanceOf[Option[String]], config.get("accountSecret").asInstanceOf[Option[String]])
    val readConsistencyLevel: String = AppConf.getConfig("assessment.metrics.cassandra.input.consistency")

    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
      .set("spark.sql.caseSensitive", AppConf.getConfig(key = "spark.sql.caseSensitive"))
    implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val tenantEncoder = Encoders.product[TenantInfo]
    CourseUtils.getTenantInfo(spark, loadData).as[TenantInfo](tenantEncoder).rdd
  }

  override def algorithm(data: RDD[TenantInfo], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[FinalOutput] = {
    val tenantConf = config("filter").asInstanceOf[Map[String, String]]

    if (!tenantConf("tenantId").isEmpty && !tenantConf("slugName").isEmpty) getContentData(tenantConf("tenantId"), tenantConf("slugName"))
    else if (!tenantConf("tenantId").isEmpty && tenantConf("slugName").isEmpty) getContentData(tenantConf("tenantId"), "Unknown")
    else data.collect().map { f => getContentData(f.id, f.slug) }.reduce((a, b) => a)
  }

  override def postProcess(data: RDD[FinalOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[FinalOutput] = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    if (data.count() > 0) {
      val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
      val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))

      val aggregated = data.map { f => f.aggregatedReport.getOrElse(AggregatedReport("", "", "", "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, "", "Unknown")) }
      val liveStatus = data.map( f => f.liveReport.getOrElse(LiveReport("", "", "", "", "", "", 0, "", "", "Unknown")))
      val nonLiveStatus = data.map( f => f.nonLiveStatusReport.getOrElse(NonLiveStatusReport("", "", "", "", "", "", "", "", "", "Unknown")))
      reportConfig.output.map { f =>
        CourseUtils.postDataToBlob(aggregated.toDF(), f, config)
        CourseUtils.postDataToBlob(liveStatus.toDF(), f, config)
        CourseUtils.postDataToBlob(nonLiveStatus.toDF(), f, config)
      }
    }
    else {JobLogger.log("No data found", None, Level.INFO)}
    data;
  }

  def getContentData(tenantId: String, slugName: String)(implicit sc: SparkContext): RDD[FinalOutput] = {
    val metrics = CommonUtil.time({
      val unitrestUtil = UnirestUtil
      val contentResponse = TextbookUtils.getContentDataList(tenantId, unitrestUtil)

      if (contentResponse.count > 0) {
        val contentData = sc.parallelize(contentResponse.content)

        val aggregatedReportRDD = getAggregatedReport(contentData, slugName)
        val liveStatusReportRDD = getLiveStatusReport(contentData, slugName)
        val nonLiveStatusReportRDD = getNonLiveStatusReport(contentData, slugName)

        val aggregatedMappingRDD = aggregatedReportRDD.map(f => (f.identifier, f))
        val liveStatusMappingRDD = liveStatusReportRDD.map(f => (f.identifier, f))
        val nonLiveStatusMappingRDD = nonLiveStatusReportRDD.map(f => (f.identifier, f))

        val joinedList = aggregatedMappingRDD.fullOuterJoin(liveStatusMappingRDD).fullOuterJoin(nonLiveStatusMappingRDD)

        val defaultAggregatedReport = AggregatedReport("", "", "", "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, "", "Unknown")
        val defualtLiveReport = LiveReport("", "", "", "", "", "", 0, "", "", "Unknown")
        joinedList
          .map { f =>
            FinalOutput(f._1,
              f._2._1.map(f => f._1.getOrElse(defaultAggregatedReport)),
              f._2._1.map(f => f._2.getOrElse(defualtLiveReport)),
              f._2._2)
          }
      } else {
        sc.emptyRDD
      }
    })
    JobLogger.log("TextbookProgressModel: For tenant: " + slugName, Option(Map("recordCount" -> metrics._2.count(), "timeTaken" -> metrics._1)), INFO)
    metrics._2.map(f => f)
  }

  def getAggregatedReport(data: RDD[ContentResult], slug: String)(implicit sc: SparkContext): RDD[AggregatedReport] = {
    data.groupBy(f => (f.board, f.medium, f.gradeLevel, f.subject, f.resourceType))
      .map { f =>
        val newGroup = scala.collection.mutable.Map(
          "board" -> f._1._1,
          "medium" -> f._1._2,
          "gradeLevel" -> f._1._3,
          "subject" -> f._1._4,
          "resourceType" -> f._1._5
        )
        f._2.groupBy { f => f.status }.map { case (x, y) => (x, y.size) } ++ f._2.groupBy { f => f.mimeType }.map { case (x, y) => (x, y.size) } ++ newGroup
      }
      .filter(f => null != f.getOrElse("board", null) || null != f.getOrElse("medium", null)  || null != f.getOrElse("gradeLevel", null) || null != f.getOrElse("subject", null))
      .map { f =>
        AggregatedReport(f.getOrElse("board", "").asInstanceOf[String], getFieldList(f.getOrElse("medium", "").asInstanceOf[Object]),
          getFieldList(f.getOrElse("gradeLevel", List()).asInstanceOf[List[String]]), getFieldList(f.getOrElse("subject", "").asInstanceOf[Object]),
          getFieldList(f.getOrElse("resourceType", "").asInstanceOf[Object]), f.getOrElse("Live", 0).asInstanceOf[Integer],
          f.getOrElse("Review", 0).asInstanceOf[Integer], f.getOrElse("Draft", 0).asInstanceOf[Integer], f.getOrElse("Unlisted", 0).asInstanceOf[Integer],
          f.getOrElse("application/vnd.ekstep.ecml-archive", 0).asInstanceOf[Integer], f.getOrElse("video/x-youtube", 0).asInstanceOf[Integer],
          f.getOrElse("video/mp4", 0).asInstanceOf[Integer] + f.getOrElse("video/webm", 0).asInstanceOf[Integer],
          f.getOrElse("application/pdf", 0).asInstanceOf[Integer] + f.getOrElse("application/epub", 0).asInstanceOf[Integer],
          f.getOrElse("application/vnd.ekstep.html-archive", 0).asInstanceOf[Integer] + f.getOrElse("application/vnd.ekstep.h5p-archive", 0).asInstanceOf[Integer],
          f.getOrElse("identifier", "").asInstanceOf[String], slug)
      }
  }

  def getLiveStatusReport(data: RDD[ContentResult], slug: String)(implicit sc: SparkContext): RDD[LiveReport] = {
    data
      .filter(f => (f.status == "Live"))
      .map { f => LiveReport(f.board, getFieldList(f.medium), getFieldList(f.gradeLevel), f.identifier, getFieldList(f.resourceType), dataFormat(f.createdOn), f.pkgVersion, f.creator, dataFormat(f.lastPublishedOn), slug) }
  }

  def getNonLiveStatusReport(data: RDD[ContentResult], slug: String)(implicit sc: SparkContext): RDD[NonLiveStatusReport] = {
    val reviewData = data.filter(f => (f.status == "Review"))
      .map { f => NonLiveStatusReport(f.board, getFieldList(f.medium), getFieldList(f.gradeLevel), f.identifier, getFieldList(f.resourceType), f.status, dataFormat(f.lastSubmittedOn), f.creator, dataFormat(f.createdOn), slug) }

    val limitedSharingData = data.filter(f => (f.status == "Unlisted"))
      .map { f => NonLiveStatusReport(f.board, getFieldList(f.medium), getFieldList(f.gradeLevel), f.identifier, getFieldList(f.resourceType), f.status, dataFormat(f.lastPublishedOn), f.creator, dataFormat(f.createdOn), slug) }

    val publishedReport = data.filter(f => (f.status == "Draft" && null != f.lastPublishedOn))
      .map { f => NonLiveStatusReport(f.board, getFieldList(f.medium), getFieldList(f.gradeLevel), f.identifier, getFieldList(f.resourceType), f.status, dataFormat(f.lastPublishedOn), f.creator, dataFormat(f.createdOn), slug) }

    val nonPublishedReport = data.filter(f => (f.status == "Draft" && null == f.lastPublishedOn))
      .map { f => NonLiveStatusReport(f.board, getFieldList(f.medium), getFieldList(f.gradeLevel), f.identifier, getFieldList(f.resourceType), f.status, dataFormat(f.createdOn), f.creator, dataFormat(f.lastPublishedOn), slug) }

    publishedReport.union(nonPublishedReport).union(reviewData).union(limitedSharingData)
  }

  def dataFormat(date: String): String = {
    if (null != date) date.split("T")(0) else ""
  }

  def getFieldList(data: Object): String = {
    if (null != data) {
      if (data.isInstanceOf[String]) data.asInstanceOf[String]
      else data.asInstanceOf[List[String]].mkString(",")
    } else ""
  }
}
