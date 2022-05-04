package org.sunbird.analytics.util

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, Params}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, RestUtil}
import org.ekstep.analytics.model.ReportConfig
import org.sunbird.analytics.model.report._

import scala.util.control.Breaks._
import scala.util.control._

case class ETBTextbookData(channel: String, identifier: String, name: String, medium: String, gradeLevel: String,
                           subject: String, status: String, createdOn: String, lastUpdatedOn: String, totalContentLinked: Int,
                           totalQRLinked: Int, totalQRNotLinked: Int, leafNodesCount: Int, leafNodeUnlinked: Int)
case class DCETextbookData(channel: String, identifier: String, name: String, medium: String, gradeLevel:String, subject: String,
                           createdOn: String, lastUpdatedOn: String, totalQRCodes: Int, contentLinkedQR: Int,
                           withoutContentQR: Int, withoutContentT1: Int, withoutContentT2: Int)
case class DialcodeExceptionData(channel: String, identifier: String, medium: String, gradeLevel: String, subject: String, name: String,
                                   l1Name: String, l2Name: String, l3Name: String, l4Name: String, l5Name: String, dialcode: String,
                                   status: String, nodeType: String, noOfContent: Int, noOfScans: Int, term: String, reportName: String)
case class ContentInformation(id: String, ver: String, ts: String, params: Params, responseCode: String,result: TextbookResult)
case class TextbookResult(count: Int, content: List[TBContentResult])

object TBConstants {
  val textbookunit = "TextBookUnit"
}

object TextBookUtils {

  def getTextBooks(config: Map[String, AnyRef])(implicit sc:SparkContext,fc: FrameworkContext): List[TextbookData] = {
    val request = JSONUtils.serialize(config.get("druidConfig").get)
    val druidQuery = JSONUtils.deserialize[DruidQueryModel](request)
    val druidResponse = DruidDataFetcher.getDruidData(druidQuery, true)

    val result = druidResponse.map(f => {
      JSONUtils.deserialize[TextbookData](f)
    })
    result.collect().toList
  }

  def getTextbookHierarchy(config: Map[String, AnyRef], textbookInfo: List[TextbookData],tenantInfo: RDD[TenantInfo],restUtil: HTTPClient)(implicit sc: SparkContext, fc: FrameworkContext): (RDD[FinalOutput]) = {
    val reportTuple = for {textbook <- textbookInfo
      baseUrl = s"${AppConf.getConfig("hierarchy.search.api.url")}${AppConf.getConfig("hierarchy.search.api.path")}${textbook.identifier}"
      finalUrl = if("Live".equals(textbook.status)) baseUrl else s"$baseUrl?mode=edit"
      response = RestUtil.get[ContentDetails](finalUrl)
      tupleData = if(null != response && "successful".equals(response.params.status)) {
        val data = response.result.content

        val dceDialcodeReport = generateDCEDialCodeReport(data, textbook)
        val etbDialcodeReport = generateETBDialcodeReport(data, textbook)

        val dialcodes = (dceDialcodeReport.map(f => f.dialcode).filter(f => f != null && f.nonEmpty) ++ etbDialcodeReport.map(f => f.dialcode).filter(f => f != null && f.nonEmpty)).distinct
        val dialcodeScanReport = getDialcodeScans(dialcodes)

        val etbReport = generateETBTextbookReport(data, textbook)
        val dceReport = generateDCETextbookReport(data, textbook)
        (etbReport, dceReport, dceDialcodeReport, etbDialcodeReport, dialcodeScanReport)
       }
       else (List(),List(),List(),List(),List())
    } yield tupleData
    val etbTextBookReport = reportTuple.filter(f => f._1.nonEmpty).map(f => f._1.head)
    val dceTextBookReport = reportTuple.filter(f => f._2.nonEmpty).map(f => f._2.head)
    val dceDialCodeReport = reportTuple.map(f => f._3).filter(f => f.nonEmpty)
    val dcereport = dceDialCodeReport.flatten
    val filteredReport = dcereport.filter(f=>(f.medium.contains(","))).distinct ++ dcereport.filter(p=>((!p.medium.contains(","))))
    val dceDialcodeReport=filteredReport.filter(f=>(f.gradeLevel.contains(","))).distinct ++ filteredReport.filter(p=>((!p.gradeLevel.contains(","))))
    val etbDialCodeReport = reportTuple.map(f => f._4).filter(f => f.nonEmpty)
    val etbreport = etbDialCodeReport.flatten
    val dialcodeScans = reportTuple.map(f => f._5).filter(f=>f.nonEmpty)
    val scans = dialcodeScans.flatten
    val dialcodeReport = dceDialcodeReport ++ etbreport

    generateWeeklyScanReport(config, scans)
    generateTextBookReport(sc.parallelize(etbTextBookReport), sc.parallelize(dceTextBookReport), sc.parallelize(dialcodeReport), tenantInfo)
  }

  def generateWeeklyScanReport(config: Map[String, AnyRef], dialcodeScans: List[WeeklyDialCodeScans])(implicit sc: SparkContext, fc: FrameworkContext) {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val configMap = config("dialcodeReportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))
    val conf = Map("reportConfig"-> configMap,"store"->config("store"),"folderPrefix"->config("folderPrefix"),"filePath"->config("filePath"),"container"->config("container"),"format"->config("format"),"key"->config("key"))
    val scansDf = sc.parallelize(dialcodeScans).toDF().dropDuplicates("dialcodes")

    reportConfig.output.map { f =>
      CourseUtils.postDataToBlob(scansDf,f,conf)
    }
  }

  def generateTextBookReport(etbTextBookReport: RDD[ETBTextbookData], dceTextBookReport: RDD[DCETextbookData], dialcodeReport: RDD[DialcodeExceptionData], tenantInfo: RDD[TenantInfo])(implicit sc: SparkContext): RDD[FinalOutput] = {
    val tenantRDD = tenantInfo.map(e => (e.id,e))
    val etbTextBook = etbTextBookReport.map(e => (e.channel,e))
    val etb=ETBTextbookData("","","","","","","","","",0,0,0,0,0)
    val etbTextBookRDD = etbTextBook.fullOuterJoin(tenantRDD).map(textbook => {
      ETBTextbookReport(textbook._2._2.getOrElse(TenantInfo("","unknown")).slug, textbook._2._1.getOrElse(etb).identifier,
        textbook._2._1.getOrElse(etb).name,textbook._2._1.getOrElse(etb).medium,textbook._2._1.getOrElse(etb).gradeLevel,
        textbook._2._1.getOrElse(etb).subject,textbook._2._1.getOrElse(etb).status,formatDate(textbook._2._1.getOrElse(etb).createdOn),formatDate(textbook._2._1.getOrElse(etb).lastUpdatedOn),
        textbook._2._1.getOrElse(etb).totalContentLinked,textbook._2._1.getOrElse(etb).totalQRLinked,textbook._2._1.getOrElse(etb).totalQRNotLinked,
        textbook._2._1.getOrElse(etb).leafNodesCount,textbook._2._1.getOrElse(etb).leafNodeUnlinked,"ETB_textbook_data")
    })
    val dceTextBook = dceTextBookReport.map(e => (e.channel,e))
    val dce = DCETextbookData("","","","","","","","",0,0,0,0,0)
    val dceTextBookRDD = dceTextBook.fullOuterJoin(tenantRDD).map(textbook => {
      DCETextbookReport(textbook._2._2.getOrElse(TenantInfo("","unknown")).slug,textbook._2._1.getOrElse(dce).identifier,
        textbook._2._1.getOrElse(dce).name,textbook._2._1.getOrElse(dce).medium,textbook._2._1.getOrElse(dce).gradeLevel,textbook._2._1.getOrElse(dce).subject,
        formatDate(textbook._2._1.getOrElse(dce).createdOn),formatDate(textbook._2._1.getOrElse(dce).lastUpdatedOn),textbook._2._1.getOrElse(dce).totalQRCodes,
        textbook._2._1.getOrElse(dce).contentLinkedQR,textbook._2._1.getOrElse(dce).withoutContentQR,textbook._2._1.getOrElse(dce).withoutContentT1,
        textbook._2._1.getOrElse(dce).withoutContentT2,"DCE_textbook_data")
    })
    val dceRDD = dceTextBookRDD.map(e => (e.identifier,e))
    val etbRDD = etbTextBookRDD.map(e => (e.identifier,e)).fullOuterJoin(dceRDD)

    val dceDialcode = dialcodeReport.map(e => (e.channel,e))
    val dialcode = DialcodeExceptionData("","","","","","","","","","","","","","",0,0,"","")
    val dceDialcodeRDD = dceDialcode.fullOuterJoin(tenantRDD).map(textbook => {
      DialcodeExceptionReport(textbook._2._2.getOrElse(TenantInfo("","unknown")).slug,textbook._2._1.getOrElse(dialcode).identifier,
        textbook._2._1.getOrElse(dialcode).medium,textbook._2._1.getOrElse(dialcode).gradeLevel,textbook._2._1.getOrElse(dialcode).subject,
        textbook._2._1.getOrElse(dialcode).name,textbook._2._1.getOrElse(dialcode).status,textbook._2._1.getOrElse(dialcode).nodeType,
        textbook._2._1.getOrElse(dialcode).l1Name,textbook._2._1.getOrElse(dialcode).l2Name,textbook._2._1.getOrElse(dialcode).l3Name,
        textbook._2._1.getOrElse(dialcode).l4Name,textbook._2._1.getOrElse(dialcode).l5Name,textbook._2._1.getOrElse(dialcode).dialcode,
        textbook._2._1.getOrElse(dialcode).noOfContent, textbook._2._1.getOrElse(dialcode).noOfScans,textbook._2._1.getOrElse(dialcode).term,textbook._2._1.getOrElse(dialcode).reportName)
    })

    val textbookReports = etbRDD.map(report => FinalOutput(report._1, report._2._1, report._2._2, null))
    val dialcodeReports = dceDialcodeRDD.map(report => FinalOutput(report.identifier, null, null, Option(report)))

    textbookReports.union(dialcodeReports)

  }

  def generateETBDialcodeReport(response: ContentInfo, textbook: TextbookData)(implicit sc: SparkContext, fc: FrameworkContext): List[DialcodeExceptionData] = {
    var dialcodeReport = List[DialcodeExceptionData]()
    val report = DialcodeExceptionData(textbook.channel,response.identifier,getString(response.medium),getString(response.gradeLevel),getString(response.subject),response.name,"","","","","","",response.status,"",response.leafNodesCount,0,"","ETB_dialcode_data")
    if(null != response && response.children.isDefined) {
      val report = parseETBDialcode(textbook,response.children.get, response, List[ContentInfo]())
        dialcodeReport = (report ++ dialcodeReport).reverse
    }
    report::dialcodeReport
  }

  def parseETBDialcode(textbookData: TextbookData, data: List[ContentInfo], response: ContentInfo, newData: List[ContentInfo], prevData: List[DialcodeExceptionData] = List())(implicit sc: SparkContext, fc: FrameworkContext): List[DialcodeExceptionData] = {
    var textbook = List[ContentInfo]()
    var etbDialcode = prevData
    var dialcode = ""

    data.map(units => {
      if(TBConstants.textbookunit.equals(units.contentType.getOrElse(""))) {
        textbook = units :: newData
        val textbookContent = ContentInfo("","","",response.medium,response.gradeLevel,response.subject,"","",Option(""),0,"",0,List(),"",Option(List[ContentInfo]()),0,"")
        val levelNames = textbook.reverse
        val dialcodeValues = textbook.lift(0).getOrElse(textbookContent).dialcodes
        val dialcodeNames = if(null != dialcodeValues) dialcodeValues.head else ""
        dialcode = dialcodeNames
        val noOfContents = units.leafNodesCount
        val dialcodes = units.dialcodes
        val nodeType = if(null != dialcodes && dialcodes.nonEmpty) "Leaf Node & QR Linked" else if(null != noOfContents && noOfContents!=0 && null != dialcodes && dialcodes.nonEmpty) "QR Linked" else "Leaf Node"
        val node = if(dialcodeNames.nonEmpty) nodeType else ""
        val nodeValue = if(response.status.equalsIgnoreCase("Draft") && node.isEmpty) "Leaf Node" else node
        val report = DialcodeExceptionData(textbookData.channel,response.identifier,getString(response.medium),getString(response.gradeLevel), getString(response.subject),response.name,levelNames.lift(0).getOrElse(textbookContent).name,levelNames.lift(1).getOrElse(textbookContent).name,levelNames.lift(2).getOrElse(textbookContent).name,levelNames.lift(3).getOrElse(textbookContent).name,levelNames.lift(4).getOrElse(textbookContent).name, dialcodeNames,response.status,nodeValue,noOfContents,0,"","ETB_dialcode_data")
        etbDialcode = report :: etbDialcode
        if(units.children.isDefined) {
          etbDialcode=parseETBDialcode(textbookData,units.children.getOrElse(List[ContentInfo]()),response,textbook,etbDialcode)
        }
      }
    })
    etbDialcode
  }

  def generateDCEDialCodeReport(response: ContentInfo, textbook: TextbookData)(implicit sc: SparkContext, fc: FrameworkContext): List[DialcodeExceptionData] = {
    var index=0
    var dialcodeReport = List[DialcodeExceptionData]()
    var chapterDialcodeReport= List[DialcodeExceptionData]()
    if(null != response && response.children.isDefined && "Live".equals(response.status)) {
      val lengthOfChapters = response.children.get.length
      if(null != response.dialcodes && null != response.leafNodesCount && response.leafNodesCount==0) {
        dialcodeReport = DialcodeExceptionData(textbook.channel, response.identifier, getString(response.medium), getString(response.gradeLevel),getString(response.subject), response.name, "","","","","",response.dialcodes(0),"","",0,0,"T1","DCE_dialcode_data") :: dialcodeReport
      }
      response.children.get.map(chapters => {
        val term = if(index<=lengthOfChapters/2) "T1"  else "T2"
        index = index+1
        val report = parseDCEDialcode(textbook,chapters.children.getOrElse(List[ContentInfo]()),response,term,chapters.name,List[ContentInfo]())
        if(null != chapters.leafNodesCount && chapters.leafNodesCount == 0) {
          val dialcodes = if(null != chapters.dialcodes) chapters.dialcodes.head else ""
          val chapterReport = DialcodeExceptionData(textbook.channel, response.identifier, getString(response.medium), getString(response.gradeLevel),getString(response.subject), response.name, chapters.name,"","","","",dialcodes,"","",0,0,term,"DCE_dialcode_data")
          chapterDialcodeReport = chapterReport :: chapterDialcodeReport
        }
        dialcodeReport = getchapterDialcodeReport(report,chapterDialcodeReport,dialcodeReport)
        chapterDialcodeReport = List[DialcodeExceptionData]()
      })
    }
    dialcodeReport
  }

  def getchapterDialcodeReport(unitReport: List[DialcodeExceptionData], chapterReport: List[DialcodeExceptionData],dialcodeReport: List[DialcodeExceptionData]): List[DialcodeExceptionData] = {
    var report = dialcodeReport
    if(unitReport.isEmpty && chapterReport.nonEmpty) { report = chapterReport ++ report }
    else { report = (unitReport ++ report).reverse
      if(chapterReport.nonEmpty && chapterReport.head.dialcode.nonEmpty && unitReport.head.dialcode.isEmpty) { report = chapterReport ++ report }
      else if(chapterReport.nonEmpty && chapterReport.head.dialcode.nonEmpty && unitReport.head.dialcode.nonEmpty && unitReport.head.dialcode!=chapterReport.head.dialcode)  { report = chapterReport ++ report }
    }
    report
  }

  def parseDCEDialcode(textbookData: TextbookData, data: List[ContentInfo], response: ContentInfo, term: String, l1: String, newData: List[ContentInfo], prevData: List[DialcodeExceptionData] = List())(implicit sc: SparkContext, fc: FrameworkContext): List[DialcodeExceptionData] =  {
    var textbook = List[ContentInfo]()
    var dceDialcode= prevData
    var dialcode = ""

    data.map(units => {
      if(TBConstants.textbookunit.equals(units.contentType.getOrElse(""))) {
        textbook = units :: newData
        if(null != units.leafNodesCount && units.leafNodesCount == 0 && null != units.dialcodes) {
          val textbookInfo = getTextBookInfo(textbook)
          val levelNames = textbookInfo._1.filter(_.nonEmpty)
          val dialcodes = textbookInfo._2.lift(0).getOrElse("")
          dialcode = dialcodes
          val report = DialcodeExceptionData(textbookData.channel, response.identifier, getString(response.medium), getString(response.gradeLevel),getString(response.subject), response.name, l1,levelNames.lift(0).getOrElse(""),levelNames.lift(1).getOrElse(""),levelNames.lift(2).getOrElse(""),levelNames.lift(3).getOrElse(""),dialcodes,"","",0,0,term,"DCE_dialcode_data")
          dceDialcode = report :: dceDialcode
          if(units.children.isDefined) {
            dceDialcode = parseDCEDialcode(textbookData,units.children.getOrElse(List[ContentInfo]()),response,term,l1,textbook,dceDialcode)
          }
        }
        else { dceDialcode = parseDCEDialcode(textbookData,units.children.getOrElse(List[ContentInfo]()),response,term,l1,textbook,dceDialcode) }
      }
    })
    dceDialcode
  }

  def getDialcodeScans(dialcodes: List[String])(implicit sc: SparkContext, fc: FrameworkContext): List[WeeklyDialCodeScans] = {
    val dialcodesDruidInQueryLength = AppConf.getConfig("etb.dialcode.druid.length").toInt

    dialcodes.sliding(dialcodesDruidInQueryLength, dialcodesDruidInQueryLength).map((dialcodeSlide: List[String]) => {
      val dialcodesStr = JSONUtils.serialize(dialcodeSlide)
      val result= if(dialcodes.nonEmpty) {
        val query = s"""{"queryType": "groupBy","dataSource": "telemetry-rollup-syncts","intervals": "Last7Days","aggregations": [{"name": "scans","type": "count"}],"dimensions": [{"fieldName": "object_id","aliasName": "dialcode"}],"filters": [{"type": "equals","dimension": "eid","value": "SEARCH"},{"type":"in","dimension":"object_id","values":$dialcodesStr},{"type":"in","dimension":"object_type","values":["DialCode","dialcode","qr","Qr"]}],"postAggregation": [],"descending": "false"}""".stripMargin
        val druidQuery = JSONUtils.deserialize[DruidQueryModel](query)
        val druidResponse = DruidDataFetcher.getDruidData(druidQuery)

        druidResponse.map(f => {
          val report = JSONUtils.deserialize[DialcodeScans](f)
          WeeklyDialCodeScans(report.date,report.dialcode,report.scans,"dialcode_scans","dialcode_counts")
        }).collect().toList
      } else List[WeeklyDialCodeScans]()
      result
    }).toList.flatten
  }

  def getTextBookInfo(data: List[ContentInfo]): (List[String],List[String]) = {
    var levelNames = List[String]()
    var dialcodes = List[String]()
    var levelCount = 5
    var parsedData = data.head
    val levelName = if(data.lift(1).isDefined) data(1).name else ""

    breakable {
      while(levelCount > 1) {
        if(TBConstants.textbookunit.equals(parsedData.contentType.getOrElse(""))) {
          if(null != parsedData.dialcodes) { dialcodes = parsedData.dialcodes(0) :: dialcodes }
          levelNames = parsedData.name :: levelNames
        }
        if(parsedData.children.getOrElse(List()).nonEmpty) { parsedData = parsedData.children.get(parsedData.children.size-1) }
        else { break() }
        levelCount = levelCount-1
      }
    }
    (levelName::levelNames.reverse,dialcodes)
  }

  def generateDCETextbookReport(response: ContentInfo, textbook: TextbookData): List[DCETextbookData] = {
    var dceReport = List[DCETextbookData]()
    if(null != response && response.children.isDefined && "Live".equals(response.status)) {
      val lengthOfChapters = response.children.get.length
      val dceTextbook = parseDCETextbook(response.identifier,"T1",response.children.get,0,0,0,0,0,0,lengthOfChapters)
      val qrLinked = dceTextbook._3
      val qrNotLinked = dceTextbook._4
      val totalQRCodes = qrLinked+qrNotLinked
      val term1NotLinked = dceTextbook._5
      val term2NotLinked = dceTextbook._6
      val medium = getString(response.medium)
      val subject = getString(response.subject)
      val gradeLevel = getString(response.gradeLevel)
      val createdOn = if(null != response.createdOn) response.createdOn.substring(0,10) else ""
      val lastUpdatedOn = if(null != response.lastUpdatedOn) response.lastUpdatedOn.substring(0,10) else ""
      val dceDf = DCETextbookData(textbook.channel,response.identifier, response.name, medium, gradeLevel, subject,createdOn, lastUpdatedOn,totalQRCodes,qrLinked,qrNotLinked,term1NotLinked,term2NotLinked)
      dceReport = dceDf::dceReport
    }
    dceReport
  }

  def parseDCETextbook(identifier: String, termValue:String, data: List[ContentInfo], index: Int, counter: Int,linkedQr: Int, qrNotLinked:Int, counterT1:Int, counterT2:Int,lengthOfChapters:Int): (Int, Int, Int, Int, Int, Int) = {
    var counterValue=counter
    var counterQrLinked = linkedQr
    var counterNotLinked = qrNotLinked
    var term1NotLinked = counterT1
    var term2NotLinked = counterT2
    var tempValue = 0
    var indexValue = index
    var term = termValue
    data.map(units => {
      if(null != units.dialcodes){
        counterValue=counterValue+1
        if(null != units.leafNodesCount && units.leafNodesCount>0) { counterQrLinked=counterQrLinked+1 }
        else {
          counterNotLinked=counterNotLinked+1
          if("T1".equals(term)) { term1NotLinked=term1NotLinked+1 }
          else { term2NotLinked = term2NotLinked+1 }
        }
      }
      if(TBConstants.textbookunit.equals(units.contentType.getOrElse(""))) {
        val output = parseDCETextbook(identifier,term,units.children.getOrElse(List[ContentInfo]()),index,counterValue,counterQrLinked,counterNotLinked,term1NotLinked,term2NotLinked,lengthOfChapters)
        indexValue = indexValue+1
        if(null != units.depth && units.depth == 1) { term = if(null != units.index && units.index<=lengthOfChapters/2) "T1"  else "T2"}
        tempValue = output._1
        counterQrLinked = output._3
        counterNotLinked = output._4
        term1NotLinked = output._5
        term2NotLinked = output._6
      }
    })
    (counterValue,tempValue,counterQrLinked,counterNotLinked,term1NotLinked,term2NotLinked)
  }

  def generateETBTextbookReport(response: ContentInfo, textbook: TextbookData): List[ETBTextbookData] = {
    var textBookReport = List[ETBTextbookData]()
    val medium = getString(response.medium)
    val subject = getString(response.subject)
    val gradeLevel = getString(response.gradeLevel)
    val createdOn = if(null != response.createdOn) response.createdOn.substring(0,10) else ""
    val lastUpdatedOn = if(null != response.lastUpdatedOn) response.lastUpdatedOn.substring(0,10) else ""
    if(null != response && response.children.isDefined) {
      val etbTextbook = parseETBTextbook(response.children.get,response,0,0,0,0)
      val qrLinkedContent = etbTextbook._1
      val qrNotLinked = etbTextbook._2
      val leafNodeswithoutContent = etbTextbook._3
      val totalLeafNodes = etbTextbook._4
      val textbookDf = ETBTextbookData(textbook.channel,response.identifier,response.name,medium,gradeLevel,subject,response.status,createdOn,lastUpdatedOn,response.leafNodesCount,qrLinkedContent,qrNotLinked,totalLeafNodes,leafNodeswithoutContent)
      textBookReport=textbookDf::textBookReport
    }
    else {
      val textbookData = ETBTextbookData(textbook.channel,response.identifier,response.name,medium,gradeLevel,subject,response.status,createdOn,lastUpdatedOn,response.leafNodesCount,0,0,1,1)
      textBookReport=textbookData::textBookReport
    }
    textBookReport
  }

  def parseETBTextbook(data: List[ContentInfo], response: ContentInfo, contentLinked: Int, contentNotLinkedQR:Int, leafNodesContent:Int, leafNodesCount:Int): (Int, Int, Int, Int) = {
    var qrLinkedContent = contentLinked
    var contentNotLinked = contentNotLinkedQR
    var leafNodeswithoutContent = leafNodesContent
    var totalLeafNodes = leafNodesCount
    data.map(units => {
      if(null != units.dialcodes){
        if(null != units.leafNodesCount && units.leafNodesCount>0) { qrLinkedContent=qrLinkedContent+1 }
        else { contentNotLinked=contentNotLinked+1 }
      }
      if(TBConstants.textbookunit.equals(units.contentType.getOrElse(""))) {
        if(units.children.isEmpty && units.leafNodesCount==0) { leafNodeswithoutContent=leafNodeswithoutContent+1 }
        if(null != units.dialcodes){ totalLeafNodes=totalLeafNodes+1 }
        val output = parseETBTextbook(units.children.getOrElse(List[ContentInfo]()),response,qrLinkedContent,contentNotLinked,leafNodeswithoutContent,totalLeafNodes)
        qrLinkedContent = output._1
        contentNotLinked = output._2
        leafNodeswithoutContent = output._3
        totalLeafNodes = output._4
      }
    })
    (qrLinkedContent,contentNotLinked,leafNodeswithoutContent,totalLeafNodes)
  }

  def getContentDataList(tenantId: String)(implicit sc:SparkContext, fc: FrameworkContext): List[TBContentResult] = {
    val request = s"""{"queryType": "groupBy","dataSource": "content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "channel","aliasName": "channel"}, {"fieldName": "identifier","aliasName": "identifier","type": "Extraction","outputType": "STRING","extractionFn": [{"type": "javascript","fn": "function(str){return str == null ? null: str.split('.')[0]}"}]}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "pkgVersion","aliasName": "pkgVersion"}, {"fieldName": "contentType","aliasName": "contentType"}, {"fieldName": "lastSubmittedOn","aliasName": "lastSubmittedOn"}, {"fieldName": "mimeType","aliasName": "mimeType"}, {"fieldName": "resourceType","aliasName": "resourceType"}, {"fieldName": "createdFor","aliasName": "createdFor"}, {"fieldName": "createdOn","aliasName": "createdOn"}, {"fieldName": "lastPublishedOn","aliasName": "lastPublishedOn"}, {"fieldName": "creator","aliasName": "creator"}, {"fieldName": "board","aliasName": "board"}, {"fieldName": "medium","aliasName": "medium"}, {"fieldName": "gradeLevel","aliasName": "gradeLevel"}, {"fieldName": "subject","aliasName": "subject"}, {"fieldName": "status","aliasName": "status"}],"filters": [{"type": "equals","dimension": "contentType","value": "Resource"}, {"type": "in","dimension": "status","values": ["Live", "Draft", "Review", "Unlisted"]}, {"type": "equals","dimension": "createdFor","value": "$tenantId"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}}""".stripMargin
    val druidQuery = JSONUtils.deserialize[DruidQueryModel](request)
    val druidResponse = DruidDataFetcher.getDruidData(druidQuery, queryAsStream = true)
    
    val result = druidResponse.map(f => JSONUtils.deserialize[TBContentResult](f))
    result.collect().toList
  }

  def getString(data: Object): String = {
    if (null != data) {
      if (data.isInstanceOf[String]) data.asInstanceOf[String]
      else data.asInstanceOf[List[String]].mkString(",")
    } else ""
  }

  def formatDate(date: String): String = {
    if(date.nonEmpty) {
      val simple = new SimpleDateFormat("yyyy-mm-dd")
      val formatedDate = new SimpleDateFormat("dd/mm/yyyy")
      formatedDate.format(simple.parse(date))
    } else ""
  }
}
