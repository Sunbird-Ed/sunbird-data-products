package org.sunbird.analytics.util

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework.Params
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, RestUtil}
import org.sunbird.analytics.model.report._
import scala.util.control.Breaks._
import scala.util.control._

case class ETBTextbookData(channel: String, identifier: String, name: String, medium: String, gradeLevel: String,
                           subject: String, status: String, createdOn: String, lastUpdatedOn: String, totalContentLinked: Integer,
                           totalQRLinked: Integer, totalQRNotLinked: Integer, leafNodesCount: Integer, leafNodeUnlinked: Integer)
case class DCETextbookData(channel: String, identifier: String, name: String, medium: String, gradeLevel:String, subject: String,
                           createdOn: String, lastUpdatedOn: String, totalQRCodes: Integer, contentLinkedQR: Integer,
                           withoutContentQR: Integer, withoutContentT1: Integer, withoutContentT2: Integer)
case class DCEDialcodeData(channel: String, identifier: String, medium: String, gradeLevel: String, subject: String, name: String,
                               l1Name: String, l2Name: String, l3Name: String, l4Name: String, l5Name: String, dialcodes: String,
                               noOfScans: Integer, term: String)
case class ETBDialcodeData(channel: String, identifier: String, medium: String, gradeLevel: String, subject: String, name: String,
                               status: String, nodeType: String, l1Name: String, l2Name: String, l3Name: String, l4Name: String, l5Name: String,
                               dialcode: String, noOfScans: Integer, noOfContent: Integer)
case class ContentInformation(id: String, ver: String, ts: String, params: Params, responseCode: String,result: TextbookResult)
case class TextbookResult(count: Int, content: List[TBContentResult])

object TBConstants {
  val textbookunit = "TextBookUnit"
}

object TextBookUtils {

  def getTextBooks(config: Map[String, AnyRef], restUtil: HTTPClient): List[TextBookInfo] = {
    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val request = JSONUtils.serialize(config.get("esConfig").get)
    val response = restUtil.post[TextBookDetails](apiURL, request)
    if(null != response && "successful".equals(response.params.status) && response.result.count>0) response.result.content else List()
  }

  def getTextbookHierarchy(textbookInfo: List[TextBookInfo],tenantInfo: RDD[TenantInfo],restUtil: HTTPClient)(implicit sc: SparkContext): (RDD[FinalOutput]) = {
    val reportTuple = for {textbook <- sc.parallelize(textbookInfo)
      baseUrl = s"${AppConf.getConfig("hierarchy.search.api.url")}${AppConf.getConfig("hierarchy.search.api.path")}${textbook.identifier}"
      finalUrl = if("Live".equals(textbook.status)) baseUrl else s"$baseUrl?mode=edit"
      response = RestUtil.get[ContentDetails](finalUrl)
      tupleData = if(null != response && "successful".equals(response.params.status)) {
      val data = response.result.content
        val dceDialcode = generateDCEDialCodeReport(data)
        val etbDialcode = generateETBDialcodeReport(data)
        val etbReport = generateETBTextbookReport(data)
        val dceReport = generateDCETextbookReport(data)
        (etbReport, dceReport, dceDialcode, etbDialcode)
       }
       else (List(),List(),List(),List())
    } yield tupleData
    val etbTextBookReport = reportTuple.filter(f => f._1.nonEmpty).map(f => f._1.head)
    val dceTextBookReport = reportTuple.filter(f => f._2.nonEmpty).map(f => f._2.head)
    val dceDialCodeReport = reportTuple.collect().toList.map(f => f._3).filter(f => f.nonEmpty)
    val dcereport = if(!dceDialCodeReport.isEmpty) dceDialCodeReport.head else List()
    val etbDialCodeReport = reportTuple.collect().toList.map(f => f._4).filter(f => f.nonEmpty)
    val etbreport = if(!etbDialCodeReport.isEmpty) etbDialCodeReport.head else List()

    generateTextBookReport(etbTextBookReport, dceTextBookReport, sc.parallelize(dcereport), sc.parallelize(etbreport), tenantInfo)
  }

  def generateTextBookReport(etbTextBookReport: RDD[ETBTextbookData], dceTextBookReport: RDD[DCETextbookData], dceDialCodeReport: RDD[DCEDialcodeData], etbDialCodeReport: RDD[ETBDialcodeData] ,tenantInfo: RDD[TenantInfo])(implicit sc: SparkContext): RDD[FinalOutput] = {
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

    val dceDialcode = dceDialCodeReport.map(e => (e.channel,e))
    val dialcode = DCEDialcodeData("","","","","","","","","","","","",0,"")
    val dceDialcodeRDD = dceDialcode.fullOuterJoin(tenantRDD).map(textbook => {
      DCEDialcodeReport(textbook._2._2.getOrElse(TenantInfo("","unknown")).slug,textbook._2._1.getOrElse(dialcode).identifier,
        textbook._2._1.getOrElse(dialcode).medium,textbook._2._1.getOrElse(dialcode).gradeLevel,textbook._2._1.getOrElse(dialcode).subject,
        textbook._2._1.getOrElse(dialcode).name,textbook._2._1.getOrElse(dialcode).l1Name,textbook._2._1.getOrElse(dialcode).l2Name,textbook._2._1.getOrElse(dialcode).l3Name,
        textbook._2._1.getOrElse(dialcode).l4Name,textbook._2._1.getOrElse(dialcode).l5Name,textbook._2._1.getOrElse(dialcode).dialcodes,textbook._2._1.getOrElse(dialcode).noOfScans,
        textbook._2._1.getOrElse(dialcode).term,"DCE_dialcode_data")
    })

    val etbDialcode = etbDialCodeReport.map(e => (e.channel,e))
    val tbdialcode = ETBDialcodeData("","","","","","","","","","","","","","",0,0)
    val etbDialcodeRDD = etbDialcode.fullOuterJoin(tenantRDD).map(textbook => {
      ETBDialcodeReport(textbook._2._2.getOrElse(TenantInfo("","unknown")).slug,textbook._2._1.getOrElse(tbdialcode).identifier,
        textbook._2._1.getOrElse(tbdialcode).medium,textbook._2._1.getOrElse(tbdialcode).gradeLevel,textbook._2._1.getOrElse(tbdialcode).subject,
        textbook._2._1.getOrElse(tbdialcode).name,textbook._2._1.getOrElse(tbdialcode).status,textbook._2._1.getOrElse(tbdialcode).nodeType,textbook._2._1.getOrElse(tbdialcode).l1Name,
        textbook._2._1.getOrElse(tbdialcode).l2Name,textbook._2._1.getOrElse(tbdialcode).l3Name, textbook._2._1.getOrElse(tbdialcode).l4Name,textbook._2._1.getOrElse(tbdialcode).l5Name,
        textbook._2._1.getOrElse(tbdialcode).dialcode,textbook._2._1.getOrElse(tbdialcode).noOfScans,
        textbook._2._1.getOrElse(tbdialcode).noOfContent,"ETB_dialcode_data")
    })
    val reportRDD = dceDialcodeRDD.map(e => (e.identifier,e))
    val dialcodeRDD = etbDialcodeRDD.map(e => (e.identifier,e)).fullOuterJoin(reportRDD)

    val textbookReports = etbRDD.map(report => FinalOutput(report._1, report._2._1, report._2._2, null, null))
    val dialcodeReports = dialcodeRDD.map(report => FinalOutput(report._1, null, null, report._2._2, report._2._1))

    textbookReports.union(dialcodeReports)

  }

  def generateETBDialcodeReport(response: ContentInfo): List[ETBDialcodeData] = {
    var index=0
    var dialcodeReport = List[ETBDialcodeData]()
    val report = ETBDialcodeData(response.channel,response.identifier,getString(response.medium),getString(response.gradeLevel),getString(response.subject),response.name,response.status,"","","","","","","",0,response.leafNodesCount)
    if(null != response && response.children.isDefined) {
      val lengthOfChapters = response.children.get.length
      response.children.get.map(chapters => {
        val term = if(index<=lengthOfChapters/2) "T1"  else "T2"
        index = index+1
        val report = parseETBDialcode(chapters.children.getOrElse(List[ContentInfo]()),chapters.leafNodesCount,chapters.dialcodes,response,term,chapters.name,List[ContentInfo]())
        dialcodeReport = (report ++ dialcodeReport).reverse
      })
    }
    report::dialcodeReport
  }

  def parseETBDialcode(data: List[ContentInfo], noOfContents: Integer,dialcodes: List[String],response: ContentInfo, term: String, l1: String, newData: List[ContentInfo], prevData: List[ETBDialcodeData] = List()): List[ETBDialcodeData] = {
    var textbook = List[ContentInfo]()
    var dceTextbook = prevData

    data.map(units => {
      if(TBConstants.textbookunit.equals(units.contentType.getOrElse(""))) {
        textbook = units :: newData
        if(null != units.leafNodesCount && units.leafNodesCount == 0) {
          val textbookInfo = getTextBookInfo(textbook)
          val levelNames = textbookInfo._1
          val dialcodeInfo = textbookInfo._2
          val nodeType = if(null != noOfContents && noOfContents==0) "Leaf Node" else if(null != dialcodes && dialcodes.nonEmpty) "Leaf Node & QR Linked" else "QR Linked"
          val report = ETBDialcodeData(response.channel,response.identifier,getString(response.medium),getString(response.gradeLevel),getString(response.subject),response.name,response.status,nodeType,l1,levelNames.lift(0).getOrElse(""),levelNames.lift(1).getOrElse(""),levelNames.lift(2).getOrElse(""),levelNames.lift(3).getOrElse(""),dialcodeInfo.lift(0).getOrElse(""),0,noOfContents)
          dceTextbook = report :: dceTextbook
        }
        else { dceTextbook = parseETBDialcode(units.children.getOrElse(List[ContentInfo]()),noOfContents,dialcodes,response,term,l1,textbook,dceTextbook) }
      }
    })
    dceTextbook
  }

  def generateDCEDialCodeReport(response: ContentInfo): List[DCEDialcodeData] = {
    var index=0
    var dialcodeReport = List[DCEDialcodeData]()
    if(null != response && response.children.isDefined && "Live".equals(response.status)) {
      val lengthOfChapters = response.children.get.length
      response.children.get.map(chapters => {
        val term = if(index<=lengthOfChapters/2) "T1"  else "T2"
        index = index+1
        val report = parseDCEDialcode(chapters.children.getOrElse(List[ContentInfo]()),response,term,chapters.name,List[ContentInfo]())
        dialcodeReport = (report ++ dialcodeReport).reverse
      })
    }
    dialcodeReport
  }

  def parseDCEDialcode(data: List[ContentInfo], response: ContentInfo, term: String, l1: String, newData: List[ContentInfo], prevData: List[DCEDialcodeData] = List()): List[DCEDialcodeData] = {
    var textbook = List[ContentInfo]()
    var dceTextbook = prevData

    data.map(units => {
      if(TBConstants.textbookunit.equals(units.contentType.getOrElse(""))) {
        textbook = units :: newData
        if(null != units.leafNodesCount && units.leafNodesCount == 0) {
          val textbookInfo = getTextBookInfo(textbook)
          val levelNames = textbookInfo._1
          val dialcodes = textbookInfo._2
          val report = DCEDialcodeData(response.channel, response.identifier, getString(response.medium), getString(response.gradeLevel),getString(response.subject),response.name, l1, levelNames.lift(0).getOrElse(""),levelNames.lift(1).getOrElse(""),levelNames.lift(2).getOrElse(""),levelNames.lift(3).getOrElse(""),dialcodes.lift(0).getOrElse(""),0,term)
          dceTextbook = report :: dceTextbook
        }
        else { dceTextbook = parseDCEDialcode(units.children.getOrElse(List[ContentInfo]()),response,term,l1,textbook,dceTextbook) }
      }
    })
    dceTextbook
  }

  def getTextBookInfo(data: List[ContentInfo]): (List[String],List[String]) = {
    var levelNames = List[String]()
    var dialcodes = List[String]()
    var levelCount = 5
    var parsedData = data(data.size-1)

    breakable {
      while(levelCount > 1) {
        if(TBConstants.textbookunit.equals(parsedData.contentType.getOrElse(""))) {
          if(null != parsedData.dialcodes) { dialcodes = parsedData.dialcodes(0) :: dialcodes }
          levelNames = parsedData.name :: levelNames
        }
        if(parsedData.children.size==0) { break }
        else { parsedData = parsedData.children.get(parsedData.children.size-1) }
        levelCount = levelCount-1
      }
    }
    (levelNames.reverse,dialcodes)
  }

  def generateDCETextbookReport(response: ContentInfo): List[DCETextbookData] = {
    var index=0
    var dceReport = List[DCETextbookData]()
    if(null != response && response.children.isDefined && "Live".equals(response.status)) {
      val lengthOfChapters = response.children.get.length
      val term = if(index<=lengthOfChapters/2) "T1"  else "T2"
      index = index+1
      val dceTextbook = parseDCETextbook(response.children.get,term,0,0,0,0,0)
      val totalQRCodes = dceTextbook._2
      val qrLinked = dceTextbook._3
      val qrNotLinked = dceTextbook._4
      val term1NotLinked = dceTextbook._5
      val term2NotLinked = dceTextbook._6
      val medium = getString(response.medium)
      val subject = getString(response.subject)
      val gradeLevel = getString(response.gradeLevel)
      val createdOn = if(null != response.createdOn) response.createdOn.substring(0,10) else ""
      val lastUpdatedOn = if(null != response.lastUpdatedOn) response.lastUpdatedOn.substring(0,10) else ""
      val dceDf = DCETextbookData(response.channel,response.identifier, response.name, medium, gradeLevel, subject,createdOn, lastUpdatedOn,totalQRCodes,qrLinked,qrNotLinked,term1NotLinked,term2NotLinked)
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
      val medium = getString(response.medium)
      val subject = getString(response.subject)
      val gradeLevel = getString(response.gradeLevel)
      val createdOn = if(null != response.createdOn) response.createdOn.substring(0,10) else ""
      val lastUpdatedOn = if(null != response.lastUpdatedOn) response.lastUpdatedOn.substring(0,10) else ""
      val textbookDf = ETBTextbookData(response.channel,response.identifier,response.name,medium,gradeLevel,subject,response.status,createdOn,lastUpdatedOn,response.leafNodesCount,qrLinkedContent,qrNotLinked,totalLeafNodes,leafNodeswithoutContent)
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

    def getContentDataList(tenantId: String, unirest: UnirestClient)(implicit sc: SparkContext): TextbookResult = {
    implicit val sqlContext = new SQLContext(sc)
    val url = Constants.COMPOSITE_SEARCH_URL
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
    val header = new util.HashMap[String, String]()
    header.put("Content-Type", "application/json")
    val response = unirest.post(url, request, Option(header))
    JSONUtils.deserialize[ContentInformation](response).result
  }

  def getString(data: Object): String = {
    if (null != data) {
      if (data.isInstanceOf[String]) data.asInstanceOf[String]
      else data.asInstanceOf[List[String]].mkString(",")
    } else ""
  }
}
