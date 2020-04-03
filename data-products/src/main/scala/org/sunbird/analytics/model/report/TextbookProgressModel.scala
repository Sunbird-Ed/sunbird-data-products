package org.sunbird.analytics.model.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.model.ReportConfig
import org.sunbird.analytics.util.CourseUtils.loadData
import org.sunbird.analytics.util.{CourseUtils, TextBookUtils, UnirestUtil}
import org.sunbird.cloud.storage.conf.AppConf

//Tenant Information from cassandra
case class TenantInformation(id: String, slug: String) extends AlgoInput

//Textbook information from composite-search
case class TBContentResult(channel: String, identifier: String, board: String, gradeLevel: List[String], medium: Object, subject: Object, status: String, creator: String, lastPublishedOn: String, lastSubmittedOn: String, createdFor: List[String], createdOn: String, contentType: String, mimeType: String, resourceType: Object, pkgVersion: Integer)

//Aggregated Report for each tenant
case class AggregatedReport(board: String, medium: String, gradeLevel: String, subject: String, resourceType: String, live: Integer, review: Integer, draft: Integer, unlisted: Integer, application_ecml: Integer, video_youtube: Integer, video_mp4: Integer, application_pdf: Integer, application_html: Integer, identifier: String, slug: String, reportName: String = "Aggregated Report")

//Live Report for each tenant
case class TBReport(board: String, medium: String, gradeLevel: String, resourceType: String, createdOn: String, pkgVersion: Option[Integer] = None, creator: String, lastPublishedOn: Option[String],status: Option[String] = None,pendingInCurrentStatus: Option[String] = None , slug: String, reportName: String)

object TextbookProgressModel extends IBatchModelTemplate[Empty, TenantInformation, Empty, Empty] with Serializable {

  implicit val className: String = "org.sunbird.analytics.model.TextbookProgressModel"

  override def name: String = "TextbookProgressModel"

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[TenantInformation] = {
    CommonUtil.setStorageConf(config.getOrElse("store", "local").toString, config.get("accountKey").asInstanceOf[Option[String]], config.get("accountSecret").asInstanceOf[Option[String]])
    val readConsistencyLevel: String = AppConf.getConfig("assessment.metrics.cassandra.input.consistency")

    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
      .set("spark.sql.caseSensitive", AppConf.getConfig(key = "spark.sql.caseSensitive"))
    implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val tenantEncoder = Encoders.product[TenantInformation]
    CourseUtils.getTenantInfo(spark, loadData).as[TenantInformation](tenantEncoder).rdd
  }

  override def algorithm(data: RDD[TenantInformation], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    val tenantConf = config("filter").asInstanceOf[Map[String, String]]
    if(!tenantConf("tenantId").isEmpty) getContentData(tenantConf("tenantId"), tenantConf("slugName"), config)
    else data.collect().map { f => getContentData(f.id, f.slug, config)}
    sc.emptyRDD
  }

  override def postProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    data;
  }

  def getContentData(tenantId: String, slugName: String, config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
    val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))

    implicit val sqlContext = new SQLContext(sc)
    val metrics = CommonUtil.time({
      val unitrestUtil = UnirestUtil
      val contentResponse = TextBookUtils.getContentDataList(tenantId, unitrestUtil)

      if (contentResponse.count > 0) {
        val slug = if (slugName == null || slugName.isEmpty) "Unknown" else slugName
        val contentData = sc.parallelize(contentResponse.content)

        val aggregatedReportDf = getAggregatedReport(contentData, slug)
          .na.fill("")

        val liveStatusReportDf = getLiveStatusReport(contentData, slug)
          .drop("status","pendingInCurrentStatus")
          .na.fill("Missing Metadata")

        val nonLiveStatusReportDf = getNonLiveStatusReport(contentData, slug)
          .drop("lastPublishedOn", "pkgVersion")
          .na.fill("Missing Metadata")

        reportConfig.output.map { f =>
          CourseUtils.postDataToBlob(aggregatedReportDf,f,config)
          CourseUtils.postDataToBlob(liveStatusReportDf,f,config)
          CourseUtils.postDataToBlob(nonLiveStatusReportDf,f,config)
        }
      }
      else {
        JobLogger.log("No data found for the tenant: " + slugName, None, Level.INFO)
      }
    })
    JobLogger.log("TextbookProgressModel: For tenant: " + slugName, Option(Map("timeTaken" -> metrics._1)), Level.INFO)
  }

  def getAggregatedReport(data: RDD[TBContentResult], slug: String)(implicit sc: SparkContext): DataFrame = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

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
      }.toDF
  }

  def getLiveStatusReport(data: RDD[TBContentResult], slug: String)(implicit sc: SparkContext): DataFrame = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    data
      .filter(f => (f.status == "Live"))
      .map { f => TBReport(f.board, getFieldList(f.medium), getFieldList(f.gradeLevel), getFieldList(f.resourceType), dataFormat(f.createdOn), Option(f.pkgVersion), f.creator, Option(dataFormat(f.lastPublishedOn)), None, None, slug, "Live_Report") }
      .toDF
  }

  def getNonLiveStatusReport(data: RDD[TBContentResult], slug: String)(implicit sc: SparkContext): DataFrame = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val reviewData = data.filter(f => (f.status == "Review"))
      .map { f => TBReport(f.board, getFieldList(f.medium), getFieldList(f.gradeLevel), getFieldList(f.resourceType), dataFormat(f.createdOn), None, f.creator, None, Option(f.status), Option(dataFormat(f.lastSubmittedOn)), slug, "Non_Live_Status") }

    val limitedSharingData = data.filter(f => (f.status == "Unlisted"))
      .map { f => TBReport(f.board, getFieldList(f.medium), getFieldList(f.gradeLevel), getFieldList(f.resourceType), dataFormat(f.createdOn), None, f.creator, None, Option(f.status), Option(dataFormat(f.lastPublishedOn)), slug, "Non_Live_Status") }

    val publishedReport = data.filter(f => (f.status == "Draft" && null != f.lastPublishedOn))
      .map { f => TBReport(f.board, getFieldList(f.medium), getFieldList(f.gradeLevel), getFieldList(f.resourceType),dataFormat(f.createdOn), None, f.creator,None, Option(f.status), Option(dataFormat(f.lastPublishedOn)), slug, "Non_Live_Status") }

    val nonPublishedReport = data.filter(f => (f.status == "Draft" && null == f.lastPublishedOn))
      .map { f => TBReport(f.board, getFieldList(f.medium), getFieldList(f.gradeLevel), getFieldList(f.resourceType),dataFormat(f.lastPublishedOn), None, f.creator,None, Option(f.status), Option(dataFormat(f.createdOn)), slug, "Non_Live_Status") }

    publishedReport.union(nonPublishedReport).union(reviewData).union(limitedSharingData).toDF()
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
