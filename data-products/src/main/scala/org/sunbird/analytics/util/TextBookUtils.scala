package org.sunbird.analytics.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils}
import org.sunbird.analytics.model.report.{TextBookDetails, TextBookInfo, ETB_textbook_report, ContentDetails, TenantInfo, DCE_textbook_report, FinalOutput, ContentInfo}

case class ETB_textbook_data(channel: String, identifier: String, name: String, medium: List[String], gradeLevel: List[String],
                             subject: List[String], status: String, createdOn: String, lastUpdatedOn: String, totalContentLinked: Integer,
                             totalQRLinked: Integer, totalQRNotLinked: Integer, leafNodesCount: Integer, leafNodeUnlinked: Integer)

case class DCE_textbook_data(channel: String, identifier: String, name: String, medium: List[String], gradeLevel: List[String], subject: List[String],
                             createdOn: String, lastUpdatedOn: String, totalQRCodes: Integer, contentLinkedQR: Integer,
                             withoutContentQR: Integer, withoutContentT1: Integer, withoutContentT2: Integer)

object TextBookUtils {

  def getTextBooks(config: Map[String, AnyRef], restUtil: HTTPClient): List[TextBookInfo] = {
    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val request = JSONUtils.serialize(config.get("esConfig").get)
    val response = restUtil.post[TextBookDetails](apiURL, request).result.content
    response
  }

  def getTextbookHierarchy(textbookInfo: List[TextBookInfo],tenantInfo: RDD[TenantInfo],restUtil: HTTPClient)(implicit sc: SparkContext): (RDD[FinalOutput]) = {
    var etbTextBookReport = List[ETB_textbook_data]()
    var dceTextBookReport = List[DCE_textbook_data]()

    textbookInfo.map(textbook => {
      var apiUrl = ""
      if(textbook.status == "Live") { apiUrl = "https://dev.sunbirded.org/api/course/v1/hierarchy/"+textbook.identifier }
      else { apiUrl = "https://dev.sunbirded.org/api/course/v1/hierarchy/"+textbook.identifier+"?mode=edit" }

      val response = restUtil.get[ContentDetails](apiUrl)

      if(response.params.status=="successful") {
        val data = response.result.content

        val etbTextBookResponse = generateETBTextbookReport(data)
        etbTextBookReport = etbTextBookReport ++ etbTextBookResponse

        val dceTextBookResponse = generateDCETextbookReport(data)
        dceTextBookReport = dceTextBookReport ++ dceTextBookResponse
      }
    })
    val tenantRDD = tenantInfo.map(e=>(e.id,e))

    val etbTextBook = sc.parallelize(etbTextBookReport).map(e=>(e.channel,e))
    val etb=ETB_textbook_data("","","",List(""),List(""),List(""),"","","",0,0,0,0,0)
    val etbTextBookRDD = etbTextBook.fullOuterJoin(tenantRDD).map(etbReport => {

      ETB_textbook_report(etbReport._2._2.getOrElse(TenantInfo("","")).slug, etbReport._2._1.getOrElse(etb).identifier,
        etbReport._2._1.getOrElse(etb).name,etbReport._2._1.getOrElse(etb).medium,etbReport._2._1.getOrElse(etb).gradeLevel,
        etbReport._2._1.getOrElse(etb).subject,etbReport._2._1.getOrElse(etb).status,etbReport._2._1.getOrElse(etb).createdOn,etbReport._2._1.getOrElse(etb).lastUpdatedOn,
        etbReport._2._1.getOrElse(etb).totalContentLinked,etbReport._2._1.getOrElse(etb).totalQRLinked,etbReport._2._1.getOrElse(etb).totalQRNotLinked,
        etbReport._2._1.getOrElse(etb).leafNodesCount,etbReport._2._1.getOrElse(etb).leafNodeUnlinked,"ETB_textbook_data")
    })

    val dceTextBook = sc.parallelize(dceTextBookReport.filter(e=>(e.totalQRCodes!=0))).map(e=>(e.channel,e))
    val dce=DCE_textbook_data("","","",List(""),List(""),List(""),"","",0,0,0,0,0)
    val dceTextBookRDD = dceTextBook.fullOuterJoin(tenantRDD).map(dceReport => {
      DCE_textbook_report(dceReport._2._2.getOrElse(TenantInfo("","")).slug,dceReport._2._1.getOrElse(dce).identifier,
        dceReport._2._1.getOrElse(dce).name,dceReport._2._1.getOrElse(dce).medium,dceReport._2._1.getOrElse(dce).gradeLevel,dceReport._2._1.getOrElse(dce).subject,
        dceReport._2._1.getOrElse(dce).createdOn,dceReport._2._1.getOrElse(dce).lastUpdatedOn,dceReport._2._1.getOrElse(dce).totalQRCodes,
        dceReport._2._1.getOrElse(dce).contentLinkedQR,dceReport._2._1.getOrElse(dce).withoutContentQR,dceReport._2._1.getOrElse(dce).withoutContentT1,
        dceReport._2._1.getOrElse(dce).withoutContentT2,"DCE_textbook_data")
    })

    val dceRDD=dceTextBookRDD.map(e=>(e.identifier,e))
    val etbRDD=etbTextBookRDD.map(e=>(e.identifier,e)).fullOuterJoin(dceRDD)
    val finalOutput = etbRDD.map(e=> FinalOutput(e._1,e._2._1,e._2._2))

    finalOutput
  }

  def generateDCETextbookReport(response: ContentInfo)(implicit sc: SparkContext): List[DCE_textbook_data] = {
    var index=0
    var totalQRCodes=0
    var qrLinked=0
    var qrNotLinked=0
    var term1NotLinked=0
    var term2NotLinked=0
    var dceReport = List[DCE_textbook_data]()

    if(response!=null && response.children.size>0 && response.status=="Live") {
      val lengthOfChapters = response.children.get.length
      val term = if(index<=lengthOfChapters/2) "T1"  else "T2"
      index = index+1

      val dceTextbook = parseDCETextbook(response.children.get,term,response,0,0,0,0,0)
      totalQRCodes = dceTextbook._2
      qrLinked = dceTextbook._3
      qrNotLinked = dceTextbook._4
      term1NotLinked = dceTextbook._5
      term2NotLinked = dceTextbook._6
      val dceDf = DCE_textbook_data(response.channel,response.identifier, response.name, response.medium.asInstanceOf[List[String]], response.gradeLevel, response.subject.asInstanceOf[List[String]],response.createdOn.substring(0,10), response.lastUpdatedOn.substring(0,10),totalQRCodes,qrLinked,qrNotLinked,term1NotLinked,term2NotLinked)
      dceReport = dceDf::dceReport
    }
    dceReport
  }

  def parseDCETextbook(data: List[ContentInfo], term: String, response: ContentInfo, counter: Integer,linkedQr: Integer, qrNotLinked:Integer, counterT1:Integer, counterT2:Integer)(implicit sc: SparkContext): (Integer,Integer,Integer,Integer,Integer,Integer) = {
    var counterValue=counter
    var counterQrLinked = linkedQr
    var counterNotLinked = qrNotLinked
    var term1NotLinked = counterT1
    var term2NotLinked = counterT2
    var tempValue = 0

    data.map(units => {
      if(units.dialcodes!=null){
        counterValue=counterValue+1

        if(units.leafNodesCount>0) { counterQrLinked=counterQrLinked+1 }
        else {
          counterNotLinked=counterNotLinked+1
          if(term == "T1") { term1NotLinked=term1NotLinked+1 }
          else { term2NotLinked = term2NotLinked+1 }
        }
      }
      if(units.contentType.get== "TextBookUnit"){
        val output = parseDCETextbook(units.children.getOrElse(List[ContentInfo]()),term,response,counterValue,counterQrLinked,counterNotLinked,term1NotLinked,term2NotLinked)
        tempValue = output._1
        counterQrLinked = output._3
        counterNotLinked = output._4
        term1NotLinked = output._5
        term2NotLinked = output._6
      }
    })
    (counterValue,tempValue,counterQrLinked,counterNotLinked,term1NotLinked,term2NotLinked)
  }

  def generateETBTextbookReport(response: ContentInfo)(implicit sc: SparkContext): List[ETB_textbook_data] = {
    var qrLinkedContent = 0
    var qrNotLinked = 0
    var totalLeafNodes = 0
    var leafNodeswithoutContent = 0
    var textBookReport = List[ETB_textbook_data]()

    if(response!=null && response.children.size>0) {
      val etbTextbook = parseETBTextbook(response.children.get,response,0,0,0,0)
      qrLinkedContent = etbTextbook._1
      qrNotLinked = etbTextbook._2+1
      leafNodeswithoutContent = etbTextbook._3
      totalLeafNodes = etbTextbook._4
      val textbookDf = ETB_textbook_data(response.channel,response.identifier,response.name,response.medium.asInstanceOf[List[String]],response.gradeLevel,response.subject.asInstanceOf[List[String]],response.status,response.createdOn.substring(0,10),response.lastUpdatedOn.substring(0,10),response.leafNodesCount,qrLinkedContent,qrNotLinked,totalLeafNodes,leafNodeswithoutContent)
      textBookReport=textbookDf::textBookReport
    }
    textBookReport
  }

  def parseETBTextbook(data: List[ContentInfo], response: ContentInfo, contentLinked: Integer, contentNotLinkedQR:Integer, leafNodesContent:Integer, leafNodesCount:Integer)(implicit sc: SparkContext): (Integer,Integer,Integer,Integer) = {
    var qrLinkedContent = contentLinked
    var contentNotLinked = contentNotLinkedQR
    var leafNodeswithoutContent = leafNodesContent
    var totalLeafNodes = leafNodesCount

    data.map(units => {
      if(units.children.size==0){ totalLeafNodes=totalLeafNodes+1 }
      if(units.children.size==0 && units.leafNodesCount==0) { leafNodeswithoutContent=leafNodeswithoutContent+1 }
      if(units.dialcodes!=null){
        if(units.leafNodesCount>0) { qrLinkedContent=qrLinkedContent+1 }
        else { contentNotLinked=contentNotLinked+1 }
      }

      if(units.contentType.get=="TextBookUnit") {
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
