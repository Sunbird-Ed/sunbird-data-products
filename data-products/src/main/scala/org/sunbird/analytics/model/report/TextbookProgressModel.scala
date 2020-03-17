package org.ekstep.analytics.model.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, RestUtil}
import org.ekstep.analytics.util.CourseUtils.loadData
import org.ekstep.analytics.util.{Constants, CourseUtils}
import org.sunbird.cloud.storage.conf.AppConf

import scala.io.Source

case class ContentInfo(id: String, ver: String, ts: String, params: Params, responseCode: String,result: Result)
case class Result(count: Int, content: List[ContentResult])
case class Facets(name: String, count: Int)

case class ContentResult(channel: String,identifier: String, board: String, gradeLevel: List[String], medium: String, subject: String,
                         status: String, creator: String,lastPublishedOn: String,lastSubmittedOn: String, createdFor: List[String],
                         createdOn: String, contentType: String, mimeType: String,
                         resourceType: String, pkgVersion: Integer)

case class AggregatedReport (board: String, medium: String, gradeLevel: Seq[String], subject: String, resourceType: String,
                             live: Integer, review: Integer, draft: Integer, unlisted: Integer,
                             application_ecml: Integer, vedio_youtube: Integer, video_mp4: Integer, application_pdf: Integer,
                             application_html: Integer,identifier: String)

case class LiveReport (board: String, medium: String,gradeLevel: List[String], identifier: String,resourceType: String, createdOn: String,pkgVersion: Integer, creator: String,
                     lastPublishedOn: String)

case class NonLiveStatusReport(board: String, medium: String,gradeLevel: List[String], identifier: String,resourceType: String, status: String, pendingInCurrentStatus: String,
                                creator: String, createdOn: String)

case class FinalOutput(identifier: String, aggregatedReport: Option[AggregatedReport], liveReport: Option[LiveReport], nonLiveStatusReport: Option[NonLiveStatusReport]) extends AlgoOutput with Output

case class TenantInfo(id: String, slug: String) extends AlgoInput

object TextbookProgressModel extends IBatchModelTemplate[Empty , TenantInfo, FinalOutput, FinalOutput] with Serializable {

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
    val data = CourseUtils.getTenantInfo(spark, loadData)
    val id = data.as[TenantInfo](tenantEncoder).rdd
    id

  }

  override def algorithm(data: RDD[TenantInfo], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[FinalOutput] = {
    val result = data.collect().map{f =>
     val finalResult = getContentData(f.id, f.slug, config)
      println("finalresult: " + finalResult)
      finalResult
    }
    println("result: " + result)
    sc.emptyRDD
  }

  override def postProcess(events: RDD[FinalOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[FinalOutput] = {
    events;
  }


  def getContentData(tenantId: String, slugName: String, config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[FinalOutput] = {

    implicit val sqlContext = new SQLContext(sc)
    val contentResponse = getContentDataList(tenantId)
    if(contentResponse.count > 0) {
      val contentData = contentResponse.content

      val aggregatedReportRDD = getAggregatedReport(contentData,config)
      val liveStatusReportRDD = getLiveStatusReport(contentData, config)
      val nonLiveStatusReportRDD = getNonLiveStatusReport(contentData, config)

      val aggregatedMappingRDD = aggregatedReportRDD.map(f => (f.identifier,f))
      val liveStatusMappingRDD = liveStatusReportRDD.map(f => (f.identifier,f))
      val nonLiveStatusMappingRDD = nonLiveStatusReportRDD.map(f => (f.identifier,f))

      val joinedList_1 = aggregatedMappingRDD.fullOuterJoin(liveStatusMappingRDD)
      val joinedList_2 = joinedList_1.fullOuterJoin(nonLiveStatusMappingRDD)

      val finalOutput = joinedList_2
        .map{f => FinalOutput(f._1, f._2._1.get._1, f._2._1.get._2, f._2._2)}
      finalOutput
    }
    else sc.emptyRDD
  }

  def getContentDataList(tenantId: String)(implicit sc: SparkContext): Result = {
    implicit val sqlContext = new SQLContext(sc)
    val url = Constants.COMPOSITE_SEARCH_URL
//    println("tenantId: " + tenantId)
    val request = s"""{
                    |      "request": {
                    |        "filters": {
                    |           "status": ["Live","Draft","Review","Unlisted"],
                    |          "contentType": ["Resource"],
                    |          "createdFor": "$tenantId"
                    |        },
                    |        "fields": ["channel","identifier","board","gradeLevel",
                    |          "medium","subject","status","creator","lastPublishedOn","createdFor",
                    |          "createdOn","pkgVersion","contentType",
                    |          "mimeType","resourceType", "lastSubmittedOn"
                    |        ],
                    |        "limit": 10000,
                    |        "facets": [
                    |          "status"
                    |        ]
                    |      }
                    |    }""".stripMargin

    val resultData = RestUtil.post[ContentInfo](url, request).result
    resultData
  }

  def getAggregatedReport(data: List[ContentResult], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AggregatedReport] = {
    val groupByList = data.groupBy(f => (f.board, f.medium, f.gradeLevel, f.subject, f.resourceType))
      .map{f =>
        val newGroup = scala.collection.mutable.Map(
          "board" -> f._1._1,
          "medium" -> f._1._2,
          "gradeLevel" -> f._1._3,
          "subject" -> f._1._4,
          "resourceType" -> f._1._5
        )
        val finalReportGroup = f._2.groupBy{f => f.status}.map{case (x,y) => (x,y.size)} ++ f._2.groupBy{f => f.mimeType}.map{case (x,y) => (x,y.size)} ++ newGroup
        finalReportGroup
      }
      .filter(f => f.getOrElse("board", null) != null || f.getOrElse("medium", null) != null || f.getOrElse("gradeLevel", null) != null || f.getOrElse("subject", null) != null)
      .map { f =>
        AggregatedReport(f.getOrElse("board", "").asInstanceOf[String], f.getOrElse("medium", "").asInstanceOf[String],
          f.getOrElse("gradeLevel", Seq()).asInstanceOf[Seq[String]], f.getOrElse("subject", "").asInstanceOf[String],
          f.getOrElse("resourceType", "").asInstanceOf[String], f.getOrElse("Live", 0).asInstanceOf[Integer],
          f.getOrElse("Review", 0).asInstanceOf[Integer], f.getOrElse("Draft", 0).asInstanceOf[Integer], f.getOrElse("Unlisted", 0).asInstanceOf[Integer],
          f.getOrElse("application/vnd.ekstep.ecml-archive", 0).asInstanceOf[Integer], f.getOrElse("video/x-youtube", 0).asInstanceOf[Integer],
          f.getOrElse("video/mp4", 0).asInstanceOf[Integer] + f.getOrElse("video/webm", 0).asInstanceOf[Integer],
          f.getOrElse("application/pdf", 0).asInstanceOf[Integer] + f.getOrElse("application/epub", 0).asInstanceOf[Integer],
          f.getOrElse("application/vnd.ekstep.html-archive", 0).asInstanceOf[Integer] + f.getOrElse("application/vnd.ekstep.h5p-archive", 0).asInstanceOf[Integer],
          f.getOrElse("identifier", "").asInstanceOf[String]
        )
      }.toList
    val finalAggregatedRDD = sc.parallelize(groupByList)
    finalAggregatedRDD
  }

  def getLiveStatusReport(data: List[ContentResult], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LiveReport] = {
    val filteredList = data.filter(f => (f.status == "Live"))
      .map{f =>
        LiveReport(f.board, f.medium, f.gradeLevel, f.identifier, f.resourceType, dataFormat(f.createdOn), f.pkgVersion, f.creator,dataFormat(f.lastPublishedOn))
      }
    val finalLiveRDD = sc.parallelize(filteredList)
    finalLiveRDD
  }

  def getNonLiveStatusReport(data: List[ContentResult], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[NonLiveStatusReport] = {
    val reviewData = data.filter(f => (f.status == "Review"))
      .map{f =>
        NonLiveStatusReport(f.board, f.medium, f.gradeLevel, f.identifier, f.resourceType, f.status, dataFormat(f.lastSubmittedOn), f.creator, dataFormat(f.createdOn))
      }

    val limitedSharingData = data.filter(f => (f.status == "Unlisted"))
      .map{f =>
        NonLiveStatusReport(f.board, f.medium, f.gradeLevel, f.identifier, f.resourceType, f.status, dataFormat(f.lastPublishedOn), f.creator, dataFormat(f.createdOn))
      }

    val publishedReport = data.filter(f => (f.status == "Draft" && f.lastPublishedOn != null))
      .map{f =>
        NonLiveStatusReport(f.board, f.medium, f.gradeLevel,f.identifier, f.resourceType, f.status, dataFormat(f.lastPublishedOn), f.creator, dataFormat(f.createdOn))
      }

    val nonPublishedReport = data.filter(f => (f.status == "Draft" && f.lastPublishedOn == null))
      .map{f =>
        NonLiveStatusReport(f.board, f.medium, f.gradeLevel,f.identifier, f.resourceType, f.status, dataFormat(f.createdOn), f.creator, dataFormat(f.lastPublishedOn))
      }

    val finalDraftReport = publishedReport.union(nonPublishedReport)
    val finalReviewReport = finalDraftReport.union(reviewData)
    val finalReport = finalReviewReport.union(limitedSharingData)
    val finalRDD = sc.parallelize(finalReport)
    finalRDD
  }

  def dataFormat(date: String): String = {
    if(date != null) date.split("T")(0) else ""
  }
}
