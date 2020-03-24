package org.sunbird.analytics.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, RestUtil}
import org.sunbird.analytics.model.report.{ContentDetails, ContentInfo, DCETextbookReport, ETBTextbookReport, FinalOutput, TenantInfo, TextBookDetails, TextBookInfo}
import org.sunbird.cloud.storage.conf.AppConf

case class ETBTextbookData(channel: String, identifier: String, name: String, medium: String, gradeLevel: String,
                             subject: String, status: String, createdOn: String, lastUpdatedOn: String, totalContentLinked: Integer,
                             totalQRLinked: Integer, totalQRNotLinked: Integer, leafNodesCount: Integer, leafNodeUnlinked: Integer)

case class DCETextbookData(channel: String, identifier: String, name: String, medium: String, gradeLevel:String, subject: String,
                             createdOn: String, lastUpdatedOn: String, totalQRCodes: Integer, contentLinkedQR: Integer,
                             withoutContentQR: Integer, withoutContentT1: Integer, withoutContentT2: Integer)

object TBConstants {
  val textbookunit = "TextBookUnit"
}

object TextBookUtils {

  def getTextBooks(config: Map[String, AnyRef], restUtil: HTTPClient): List[TextBookInfo] = {
    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val request = JSONUtils.serialize(config.get("esConfig").get)
    val response = restUtil.post[TextBookDetails](apiURL, request).result.content
    response
  }

  def getTextbookHierarchy(textbookInfo: List[TextBookInfo],tenantInfo: RDD[TenantInfo],restUtil: HTTPClient)(implicit sc: SparkContext): (RDD[FinalOutput]) = {
    val reportTuple = for {textbook <- sc.parallelize(textbookInfo)
      baseUrl = AppConf.getConfig("hierarchy.search.api.url")+AppConf.getConfig("hierarchy.search.api.path")+textbook.identifier
      finalUrl = if("Live".equals(textbook.status)) baseUrl else s"$baseUrl?mode=edit"
      response = RestUtil.get[ContentDetails](finalUrl)
      tupleData = if("successful".equals(response.params.status)) {
        val data = response.result.content
        val etbReport = generateETBTextbookReport(data)
        val dceReport = generateDCETextbookReport(data)
        (etbReport, dceReport)
      }
      else (List(),List())
    } yield tupleData

    val etbTextBookReport = reportTuple.filter(f => f._1.nonEmpty).map(f => f._1.head)
    val dceTextBookReport = reportTuple.filter(f => f._2.nonEmpty).map(f => f._2.head)

    generateTextBookReport(etbTextBookReport, dceTextBookReport, tenantInfo)
  }

  def generateTextBookReport(etbTextBookReport: RDD[ETBTextbookData], dceTextBookReport: RDD[DCETextbookData], tenantInfo: RDD[TenantInfo]): RDD[FinalOutput] = {
    val tenantRDD = tenantInfo.map(e => (e.id,e))
    val etbTextBook = etbTextBookReport.map(e => (e.channel,e))
    val etb=ETBTextbookData("","","","","","","","","",0,0,0,0,0)
    val etbTextBookRDD = etbTextBook.fullOuterJoin(tenantRDD).map(textbook => {
      ETBTextbookReport(textbook._2._2.getOrElse(TenantInfo("","unknown")).slug, textbook._2._1.getOrElse(etb).identifier,
        textbook._2._1.getOrElse(etb).name,textbook._2._1.getOrElse(etb).medium,textbook._2._1.getOrElse(etb).gradeLevel,
        textbook._2._1.getOrElse(etb).subject,textbook._2._1.getOrElse(etb).status,textbook._2._1.getOrElse(etb).createdOn,textbook._2._1.getOrElse(etb).lastUpdatedOn,
        textbook._2._1.getOrElse(etb).totalContentLinked,textbook._2._1.getOrElse(etb).totalQRLinked,textbook._2._1.getOrElse(etb).totalQRNotLinked,
        textbook._2._1.getOrElse(etb).leafNodesCount,textbook._2._1.getOrElse(etb).leafNodeUnlinked,"ETB_textbook_data")
    })

    val dceTextBook = dceTextBookReport.filter(e => (e.totalQRCodes!=0)).map(e => (e.channel,e))
    val dce = DCETextbookData("","","","","","","","",0,0,0,0,0)
    val dceTextBookRDD = dceTextBook.fullOuterJoin(tenantRDD).map(textbook => {
      DCETextbookReport(textbook._2._2.getOrElse(TenantInfo("","unknown")).slug,textbook._2._1.getOrElse(dce).identifier,
        textbook._2._1.getOrElse(dce).name,textbook._2._1.getOrElse(dce).medium,textbook._2._1.getOrElse(dce).gradeLevel,textbook._2._1.getOrElse(dce).subject,
        textbook._2._1.getOrElse(dce).createdOn,textbook._2._1.getOrElse(dce).lastUpdatedOn,textbook._2._1.getOrElse(dce).totalQRCodes,
        textbook._2._1.getOrElse(dce).contentLinkedQR,textbook._2._1.getOrElse(dce).withoutContentQR,textbook._2._1.getOrElse(dce).withoutContentT1,
        textbook._2._1.getOrElse(dce).withoutContentT2,"DCE_textbook_data")
    })

    val dceRDD = dceTextBookRDD.map(e => (e.identifier,e))
    val etbRDD = etbTextBookRDD.map(e => (e.identifier,e)).fullOuterJoin(dceRDD)
    etbRDD.map(e => FinalOutput(e._1,e._2._1,e._2._2))
  }

  def generateDCETextbookReport(response: ContentInfo): List[DCETextbookData] = {
    var index=0
    var dceReport = List[DCETextbookData]()

    if(response!=null && response.children.isDefined && response.status=="Live") {
      val lengthOfChapters = response.children.get.length
      val term = if(index<=lengthOfChapters/2) "T1"  else "T2"
      index = index+1

      val dceTextbook = parseDCETextbook(response.children.get,term,0,0,0,0,0)
      val totalQRCodes = dceTextbook._2
      val qrLinked = dceTextbook._3
      val qrNotLinked = dceTextbook._4
      val term1NotLinked = dceTextbook._5
      val term2NotLinked = dceTextbook._6
      val medium = if(null != response.medium) response.medium.asInstanceOf[List[String]].mkString(",") else ""
      val subject = if(null != response.subject) response.subject.asInstanceOf[List[String]].mkString(",") else ""
      val gradeLevel = if(null != response.gradeLevel) response.gradeLevel.mkString(",") else ""
      val dceDf = DCETextbookData(response.channel,response.identifier, response.name, medium, gradeLevel, subject,response.createdOn.substring(0,10), response.lastUpdatedOn.substring(0,10),totalQRCodes,qrLinked,qrNotLinked,term1NotLinked,term2NotLinked)
      dceReport = dceDf::dceReport
    }
    dceReport
  }

  def parseDCETextbook(data: List[ContentInfo], term: String, counter: Integer,linkedQr: Integer, qrNotLinked:Integer, counterT1:Integer, counterT2:Integer): (Integer,Integer,Integer,Integer,Integer,Integer) = {
    var counterValue=counter
    var counterQrLinked = linkedQr
    var counterNotLinked = qrNotLinked
    var term1NotLinked = counterT1
    var term2NotLinked = counterT2
    var tempValue = 0

    data.map(units => {
      if(null != units.dialcodes){
        counterValue=counterValue+1

        if(units.leafNodesCount>0) { counterQrLinked=counterQrLinked+1 }
        else {
          counterNotLinked=counterNotLinked+1
          if("T1".equals(term)) { term1NotLinked=term1NotLinked+1 }
          else { term2NotLinked = term2NotLinked+1 }
        }
      }
      if(TBConstants.textbookunit.equals(units.contentType.get)) {
        val output = parseDCETextbook(units.children.getOrElse(List[ContentInfo]()),term,counterValue,counterQrLinked,counterNotLinked,term1NotLinked,term2NotLinked)
        tempValue = output._1
        counterQrLinked = output._3
        counterNotLinked = output._4
        term1NotLinked = output._5
        term2NotLinked = output._6
      }
    })
    (counterValue,tempValue,counterQrLinked,counterNotLinked,term1NotLinked,term2NotLinked)
  }

  def generateETBTextbookReport(response: ContentInfo): List[ETBTextbookData] = {
    var textBookReport = List[ETBTextbookData]()

    if(null != response && response.children.isDefined) {
      val etbTextbook = parseETBTextbook(response.children.get,response,0,0,0,0)
      val qrLinkedContent = etbTextbook._1
      val qrNotLinked = etbTextbook._2
      val leafNodeswithoutContent = etbTextbook._3
      val totalLeafNodes = etbTextbook._4
      val medium = if(null != response.medium) response.medium.asInstanceOf[List[String]].mkString(",") else ""
      val subject = if(null != response.subject) response.subject.asInstanceOf[List[String]].mkString(",") else ""
      val gradeLevel = if(null != response.gradeLevel) response.gradeLevel.mkString(",") else ""
      val textbookDf = ETBTextbookData(response.channel,response.identifier,response.name,medium,gradeLevel,subject,response.status,response.createdOn.substring(0,10),response.lastUpdatedOn.substring(0,10),response.leafNodesCount,qrLinkedContent,qrNotLinked,totalLeafNodes,leafNodeswithoutContent)
      textBookReport=textbookDf::textBookReport
    }
    textBookReport
  }

  def parseETBTextbook(data: List[ContentInfo], response: ContentInfo, contentLinked: Integer, contentNotLinkedQR:Integer, leafNodesContent:Integer, leafNodesCount:Integer): (Integer,Integer,Integer,Integer) = {
    var qrLinkedContent = contentLinked
    var contentNotLinked = contentNotLinkedQR
    var leafNodeswithoutContent = leafNodesContent
    var totalLeafNodes = leafNodesCount

    data.map(units => {
      if(units.children.isEmpty){ totalLeafNodes=totalLeafNodes+1 }
      if(units.children.isEmpty && units.leafNodesCount==0) { leafNodeswithoutContent=leafNodeswithoutContent+1 }
      if(null != units.dialcodes){
        if(units.leafNodesCount>0) { qrLinkedContent=qrLinkedContent+1 }
        else { contentNotLinked=contentNotLinked+1 }
      }

      if(TBConstants.textbookunit.equals(units.contentType.get)) {
        val output = parseETBTextbook(units.children.getOrElse(List[ContentInfo]()),response,qrLinkedContent,contentNotLinked,leafNodeswithoutContent,totalLeafNodes)
        qrLinkedContent = output._1
        contentNotLinked = output._2
        leafNodeswithoutContent = output._3
        totalLeafNodes = output._4
      }
    })
    (qrLinkedContent,contentNotLinked,leafNodeswithoutContent,totalLeafNodes)
  }
}
