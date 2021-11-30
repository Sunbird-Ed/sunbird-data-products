package org.sunbird.analytics.model.report

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.{HTTPClient, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{AlgoOutput, DataFetcher, Empty, FrameworkContext, IBatchModelTemplate, JobConfig, Level, Output}
import org.ekstep.analytics.model.{OutputConfig, ReportConfig}
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.CommonUtil
import org.sunbird.analytics.util.{CourseUtils, TextBookUtils}
import org.sunbird.cloud.storage.conf.AppConf
import org.apache.spark.sql.functions._


case class TenantInfo(id: String, slug: String)
case class TenantResponse(result: TenantResult)
case class TenantResult(response: ContentList)
case class ContentList(count: Int, content: List[TenantInfo])

case class TextbookData(channel: String, identifier: String, name: String, createdFor: String, createdOn: String, lastUpdatedOn: String,
                  board: String, medium: String, gradeLevel: String, subject: String, status: String)

case class ContentDetails(params: Params, result: ContentResult)
case class Params(status: String)
case class ContentResult(content: ContentInfo)
case class ContentInfo(channel: String, board: String, identifier: String, medium: Object, gradeLevel: List[String], subject: Object,
                       name: String, status: String, contentType: Option[String], leafNodesCount: Int, lastUpdatedOn: String,
                       depth: Int, dialcodes:List[String], createdOn: String, children: Option[List[ContentInfo]], index: Int, parent: String)

// Textbook ID, Medium, Grade, Subject, Textbook Name, Textbook Status, Created On, Last Updated On, Total content linked, Total QR codes linked to content, Total number of QR codes with no linked content, Total number of leaf nodes, Number of leaf nodes with no content
case class ETBTextbookReport(slug: String, identifier: String, name: String, medium: String, gradeLevel: String,
                               subject:String, status: String, createdOn: String, lastUpdatedOn: String, totalContentLinked: Int,
                               totalQRLinked: Int, totalQRNotLinked: Int, leafNodesCount: Int, leafNodeUnlinked: Int, reportName: String)

// Textbook ID, Medium, Grade, Subject, Textbook Name, Created On, Last Updated On, Total No of QR Codes, Number of QR codes with atleast 1 linked content,	Number of QR codes with no linked content, Term 1 QR Codes with no linked content, Term 2 QR Codes with no linked content
case class DCETextbookReport(slug: String, identifier: String, name: String, medium: String, gradeLevel:String, subject: String,
                               createdOn: String, lastUpdatedOn: String, totalQRCodes: Int, contentLinkedQR: Int,
                               withoutContentQR: Int, withoutContentT1: Int, withoutContentT2: Int, reportName: String)

case class DialcodeExceptionReport(slug: String, identifier: String, medium: String, gradeLevel: String, subject: String, name: String,
                                   status: String, nodeType: String, l1Name: String, l2Name: String, l3Name: String, l4Name: String, l5Name: String, dialcode: String,
                                   noOfContent: Int, noOfScans: Int, term: String, reportName: String)

case class FinalOutput(identifier: String, etb: Option[ETBTextbookReport], dce: Option[DCETextbookReport], dialcode: Option[DialcodeExceptionReport]) extends AlgoOutput with Output
case class DialcodeScans(dialcode: String, scans: Double, date: String)
case class WeeklyDialCodeScans(date: String, dialcodes: String, scans: Double, slug: String, reportName: String)
case class DialcodeCounts(dialcode: String, scans: Double, date: String)

object ETBMetricsModel extends IBatchModelTemplate[Empty,Empty,FinalOutput,FinalOutput] with Serializable {
  implicit val className: String = "org.sunbird.analytics.model.report.ETBMetricsModel"
  override def name: String = "ETBMetricsModel"

  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[Empty] = {
    CommonUtil.setStorageConf(config.getOrElse("store", "local").toString, config.get("accountKey").asInstanceOf[Option[String]], config.get("accountSecret").asInstanceOf[Option[String]])
    sc.emptyRDD
  }

  override def algorithm(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[FinalOutput] = {
    generateReports(config)
  }

  override def postProcess(events: RDD[FinalOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[FinalOutput] = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    if(events.count() > 0) {
      val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
      val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))

      val etbTextBookReport = events.map(report => {
        if(null != report.etb && report.etb.size!=0) report.etb.get else ETBTextbookReport("","","","","","","","","",0,0,0,0,0,"")
      }).filter(textbook=> !textbook.identifier.isEmpty)

      val dceTextBookReport = events.map(report => {
        if(null != report.dce && report.dce.size!=0) report.dce.get else DCETextbookReport("","","","","","","","",0,0,0,0,0,"")
      }).filter(textbook=> !textbook.identifier.isEmpty)

      val etbDialcodeReport = events.map(report => {
        if(null != report.dialcode && report.dialcode.size!=0 && "ETB_dialcode_data".equals(report.dialcode.get.reportName)) report.dialcode.get else DialcodeExceptionReport("","","","","","","","","","","","","","",0,0,"","")
      }).filter(textbook => !textbook.identifier.isEmpty)

      val dceDialcodeReport = events.map(report => {
        if(null != report.dialcode && report.dialcode.isDefined && "DCE_dialcode_data".equals(report.dialcode.get.reportName)) report.dialcode.get else DialcodeExceptionReport("","","","","","","","","","","","","","",0,0,"","")
      }).filter(textbook=> !textbook.identifier.isEmpty && !textbook.dialcode.isEmpty)

      val scansDF = getScanCounts(config)

      reportConfig.output.map { f =>
        val reportConfig = config("reportConfig").asInstanceOf[Map[String,AnyRef]]
        val mergeConf = reportConfig.getOrElse("mergeConfig", Map()).asInstanceOf[Map[String,AnyRef]]

        val etbDf = etbTextBookReport.toDF().dropDuplicates("identifier","status")
          .orderBy('medium,split(split('gradeLevel,",")(0)," ")(1).cast("int"),'subject,'identifier,'status)
        var reportMap = updateReportPath(mergeConf, reportConfig, AppConf.getConfig("etbtextbook.filename"))
        CourseUtils.postDataToBlob(etbDf,f,config.updated("reportConfig",reportMap))

        val dceDf = dceTextBookReport.toDF().dropDuplicates()
          .orderBy('medium,split(split('gradeLevel,",")(0)," ")(1).cast("int"),'subject)
        reportMap = updateReportPath(mergeConf, reportConfig, AppConf.getConfig("dcetextbook.filename"))
        CourseUtils.postDataToBlob(dceDf,f,config.updated("reportConfig",reportMap))

        generateAggReports(etbDf, dceDf, f, List(config, reportConfig, mergeConf))

        val dialdceDF = dceDialcodeReport.toDF()
        val dialcodeDCE = dialdceDF.join(scansDF,dialdceDF.col("dialcode")===scansDF.col("dialcodes"),"left_outer")
          .drop("dialcodes","noOfScans","status","nodeType","noOfContent")
          .coalesce(1)
          .orderBy('medium,split(split('gradeLevel,",")(0)," ")(1).cast("int"),'subject,'identifier,'l1Name,'l2Name,'l3Name,'l4Name,'l5Name)
        reportMap = updateReportPath(mergeConf, reportConfig, AppConf.getConfig("dcedialcode.filename"))
        CourseUtils.postDataToBlob(dialcodeDCE,f,config.updated("reportConfig",reportMap))

        val dialetbDF = etbDialcodeReport.toDF()
        val dialcodeETB = dialetbDF.join(scansDF,dialetbDF.col("dialcode")===scansDF.col("dialcodes"),"left_outer")
          .drop("dialcodes","noOfScans","term")
          .dropDuplicates()
          .orderBy('medium,split(split('gradeLevel,",")(0)," ")(1).cast("int"),'subject,'identifier,'l1Name,'l2Name,'l3Name,'l4Name,'l5Name)
        reportMap = updateReportPath(mergeConf, reportConfig, AppConf.getConfig("etbdialcode.filename"))
        CourseUtils.postDataToBlob(dialcodeETB,f,config.updated("reportConfig",reportMap))
      }
    }
    events
  }

  def updateReportPath(mergeConf: Map[String,AnyRef], reportConfig: Map[String,AnyRef], reportPath: String): Map[String,AnyRef] = {
    val mergeMap = mergeConf map {
      case ("reportPath","dialcode_counts.csv") => "reportPath" -> reportPath
      case x => x
    }
    if(mergeMap.nonEmpty) reportConfig.updated("mergeConfig",mergeMap) else reportConfig
  }

  def generateAggReports(etbDf: DataFrame, dceDf: DataFrame, outputConf: OutputConfig, aggConf: List[Map[String, AnyRef]])(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //dce_qr_content_status_grade.csv
    val dceGradeDf = dceDf.select($"slug",$"contentLinkedQR",$"withoutContentQR",explode_outer(split('gradeLevel,",")).as("Class"))
      .groupBy("Class","slug").agg(sum("contentLinkedQR").alias("QR Codes with content"),sum("withoutContentQR").alias("QR Codes without content")).withColumn("reportName",lit("dce_qr_content_status_grade"))
      .na.fill("Unknown", Seq("Class"))
      .orderBy(split(split('Class,",")(0)," ")(1).cast("int"))
    var reportMap = updateReportPath(aggConf(2), aggConf(1), "dce_qr_content_status_grade.csv")
    CourseUtils.postDataToBlob(dceGradeDf,outputConf,aggConf.head.updated("reportConfig",reportMap))

    //dce_qr_content_status_subject.csv
    val dceSubjectDf = dceDf.select($"slug",$"contentLinkedQR",$"withoutContentQR",explode_outer(split('subject,",")).as("Subject"))
      .groupBy("Subject","slug").agg(sum("contentLinkedQR").alias("QR Codes with content"),sum("withoutContentQR").alias("QR Codes without content")).withColumn("reportName",lit("dce_qr_content_status_subject"))
      .na.fill("Unknown", Seq("Subject"))
      .orderBy("Subject")
    reportMap = updateReportPath(aggConf(2), aggConf(1), "dce_qr_content_status_subject.csv")
    CourseUtils.postDataToBlob(dceSubjectDf,outputConf,aggConf.head.updated("reportConfig",reportMap))

    //dce_qr_content_status.csv
    val dceStatusDf = dceDf.groupBy("slug").agg(sum("contentLinkedQR").alias("Count")).withColumn("Status",lit("QR Code With Content"))
      .select("Status","Count","slug").union(dceDf.groupBy("slug").agg(sum("withoutContentQR").alias("Count")).withColumn("Status",lit("QR Code Without Content"))
      .select("Status","Count","slug")).withColumn("reportName",lit("dce_qr_content_status"))
    reportMap = updateReportPath(aggConf(2), aggConf(1), "dce_qr_content_status.csv")
    CourseUtils.postDataToBlob(dceStatusDf,outputConf,aggConf.head.updated("reportConfig",reportMap))

    //etb_qr_content_status_grade.csv
    val etbGradeDf = etbDf.select($"slug",$"totalQRLinked",$"totalQRNotLinked",explode_outer(split('gradeLevel,",")).as("Class"))
      .groupBy("Class","slug").agg(sum("totalQRLinked").alias("QR Codes with content"),sum("totalQRNotLinked").alias("QR Codes without content")).withColumn("reportName",lit("etb_qr_content_status_grade"))
      .na.fill("Unknown", Seq("Class"))
      .orderBy(split(split('Class,",")(0)," ")(1).cast("int"))
    reportMap = updateReportPath(aggConf(2), aggConf(1), "etb_qr_content_status_grade.csv")
    CourseUtils.postDataToBlob(etbGradeDf,outputConf,aggConf.head.updated("reportConfig",reportMap))

    //etb_qr_content_status_subject.csv
    val etbContentStatus = etbDf.select($"slug",$"totalQRLinked",$"totalQRNotLinked",explode_outer(split('subject,",")).as("Subject"))
      .groupBy("Subject","slug").agg(sum("totalQRLinked").alias("QR Codes with content"),sum("totalQRNotLinked").alias("QR Codes without content")).withColumn("reportName",lit("etb_qr_content_status_subject"))
      .na.fill("Unknown", Seq("Subject"))
      .orderBy("Subject")
    reportMap = updateReportPath(aggConf(2), aggConf(1), "etb_qr_content_status_subject.csv")
    CourseUtils.postDataToBlob(etbContentStatus,outputConf,aggConf.head.updated("reportConfig",reportMap))

    //etb_qr_content_status.csv
    val etbStatusDf = etbDf.groupBy("slug").agg(sum("totalQRLinked").alias("Count")).withColumn("Status",lit("QR Code With Content"))
      .select("Status","Count","slug").union(etbDf.groupBy("slug").agg(sum("totalQRNotLinked").alias("Count")).withColumn("Status",lit("QR Code Without Content"))
      .select("Status","Count","slug")).withColumn("reportName",lit("etb_qr_content_status"))
    reportMap = updateReportPath(aggConf(2), aggConf(1), "etb_qr_content_status.csv")
    CourseUtils.postDataToBlob(etbStatusDf,outputConf,aggConf.head.updated("reportConfig",reportMap))

    //etb_qr_count.csv
    val etbQRCount = etbDf.withColumn("Status",when(col("totalQRLinked")+col("totalQRNotLinked")>0,"With QR Code").otherwise("Without QR Code"))
      .groupBy("Status","slug").agg(count("identifier").alias("Count")).withColumn("reportName",lit("etb_qr_count"))
    reportMap = updateReportPath(aggConf(2), aggConf(1), "etb_qr_count.csv")
    CourseUtils.postDataToBlob(etbQRCount,outputConf,aggConf.head.updated("reportConfig",reportMap))

    //etb_textbook_status_grade.csv
    val etbGradeStatus = etbDf.select($"slug",$"identifier",$"status",explode_outer(split('gradeLevel,",")).as("Class"))
      .groupBy("Class","slug").pivot(col("status"), Seq("Live","Review","Draft"))
      .agg(count("identifier")).drop("status").na.fill("Unknown", Seq("Class")).withColumn("reportName",lit("etb_textbook_status_grade"))
      .na.fill(0).orderBy(split(split('Class,",")(0)," ")(1).cast("int"))
    reportMap = updateReportPath(aggConf(2), aggConf(1), "etb_textbook_status_grade.csv")
    CourseUtils.postDataToBlob(etbGradeStatus,outputConf,aggConf.head.updated("reportConfig",reportMap))

    //etb_textbook_status_subject.csv
    val etbSubjectStatus = etbDf.select($"slug",$"identifier",$"status",explode_outer(split('subject,",")).as("Subject"))
      .groupBy("Subject","slug").pivot(col("status"), Seq("Live","Review","Draft"))
      .agg(count("identifier")).drop("status").na.fill("Unknown", Seq("Subject")).withColumn("reportName",lit("etb_textbook_status_subject"))
      .na.fill(0)
      .orderBy("Subject")
    reportMap = updateReportPath(aggConf(2), aggConf(1), "etb_textbook_status_subject.csv")
    CourseUtils.postDataToBlob(etbSubjectStatus,outputConf,aggConf.head.updated("reportConfig",reportMap))

    //etb_textbook_status.csv
    val etbTextbookStatus = etbSubjectStatus.select($"slug",expr("stack(3, 'Live', Live, 'Review', Review, 'Draft', Draft) as(Status,Total)"))
      .where("Total is not null").groupBy("Status","slug")
      .agg(sum("Total").alias("Count")).withColumn("reportName",lit("etb_textbook_status"))
    reportMap = updateReportPath(aggConf(2), aggConf(1), "etb_textbook_status.csv")
    CourseUtils.postDataToBlob(etbTextbookStatus,outputConf,aggConf.head.updated("reportConfig",reportMap))

  }

  def getScanCounts(config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): DataFrame = {
    implicit val sqlContext = new SQLContext(sc)

    val store = config("store")
    val conf = config.get("etbFileConfig").get.asInstanceOf[Map[String, AnyRef]]
    val url = store match {
      case "local" =>
        conf("filePath").asInstanceOf[String]
      case "s3" | "azure" =>
        val bucket = conf("bucket")
        val key = AppConf.getConfig("azure_storage_key")
        val file = conf("file")
        s"wasb://$bucket@$key.blob.core.windows.net/$file"
    }

    val scansCount = sqlContext.sparkSession.read
      .option("header","true")
      .csv(url)

    val scansDF = scansCount.selectExpr("Date", "dialcodes", "cast(scans as int) scans")
    scansDF.groupBy(scansDF("dialcodes")).sum("scans")
  }

  def generateReports(config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): (RDD[FinalOutput]) = {
    val metrics = CommonUtil.time({
      val textBookInfo = TextBookUtils.getTextBooks(config)
      val tenantInfo = getTenantInfo(config, RestUtil)
      TextBookUtils.getTextbookHierarchy(config, textBookInfo, tenantInfo, RestUtil)
    })
    JobLogger.log("ETBMetricsModel: ",Option(Map("recordCount" -> metrics._2.count(), "timeTaken" -> metrics._1)), Level.INFO)
    metrics._2
  }

  def getTenantInfo(config: Map[String, AnyRef], restUtil: HTTPClient)(implicit sc: SparkContext):  RDD[TenantInfo] = {
    val url = Constants.ORG_SEARCH_URL
    val tenantConf = config.get("tenantConfig").get.asInstanceOf[Map[String, String]]
    val filters = if(tenantConf.get("tenantId").get.nonEmpty) s"""{"id":"${tenantConf.get("tenantId").get}"}""".stripMargin
    else if(tenantConf.get("slugName").get.nonEmpty) s"""{"slug":"${tenantConf.get("slugName").get}"}""".stripMargin
    else s"""{"isTenant":"true"}""".stripMargin

    val tenantRequest = s"""{
            |    "params": { },
            |    "request":{
            |        "filters": $filters,
            |        "offset": 0,
            |        "limit": 1000,
            |        "fields": ["id", "channel", "slug", "orgName"]
            |    }
            |}""".stripMargin
    sc.parallelize(restUtil.post[TenantResponse](url, tenantRequest).result.response.content)
  }

}
