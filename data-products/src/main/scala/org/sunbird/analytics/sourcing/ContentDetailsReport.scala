package org.sunbird.analytics.sourcing

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, collect_list, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{DruidQueryModel, FrameworkContext, IJob, JobConfig, Level}
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.analytics.sourcing.FunnelReport.{connProperties, programTable, url}
import org.sunbird.analytics.sourcing.SourcingMetrics.{getStorageConfig, getTenantInfo, saveReportToBlob}

case class TextbookDetails(identifier: String, name: String, board: String, medium: String, gradeLevel: String, subject: String, acceptedContents: String, rejectedContents: String, programId: String)
case class ContentDetails(identifier: String, collectionId: String, name: String, primaryCategory: String, unitIdentifiers: String, createdBy: String, creator: String, mimeType: String, prevStatus: String, status: String)
case class ContentReport(programId: String, board: String, medium: String, gradeLevel: String, subject: String, name: String,
                         identifier: String, chapterId: String, contentName: String, contentId: String, contentType: String,
                         mimeType: String, contentStatus: String, creator: String, createdBy: String)

object ContentDetailsReport extends optional.Application with IJob with BaseReportsJob {
  implicit val className = "org.sunbird.analytics.sourcing.ContentDetailsReport"
  val jobName: String = "Content Details Job"

  // $COVERAGE-OFF$ Disabling scoverage for main method
  def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    JobLogger.log(s"Started execution - $jobName",None, Level.INFO)
    implicit val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    implicit val spark = openSparkSession(jobConfig)

    try {
      val res = CommonUtil.time(execute())
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1)))
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  // $COVERAGE-ON$ Enabling scoverage for all other functions
  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) = {
    implicit val sc = spark.sparkContext
    import spark.implicits._
    implicit val modelParams = config.modelParams.get
    val contentQuery = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(modelParams.getOrElse("contentQuery","")))
    val response = DruidDataFetcher.getDruidData(contentQuery, true)
    val contents = response.map(f => JSONUtils.deserialize[ContentDetails](f)).toDF().withColumnRenamed("identifier","contentId")
      .withColumnRenamed("name","contentName").persist(StorageLevel.MEMORY_ONLY)
    JobLogger.log(s"Total contents - ${contents.count()}",None, Level.INFO)
    process(contents)
  }

  def process(contents: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) = {
    implicit val sc = spark.sparkContext
    implicit val modelParams = config.modelParams.get
    val tenantId = modelParams.getOrElse("tenantId","").asInstanceOf[String]
    val slug = modelParams.getOrElse("slug","").asInstanceOf[String]

    if(tenantId.nonEmpty && slug.nonEmpty) {
      generateTenantReport(tenantId, slug, contents)
    } else {
      val tenantInfo = getTenantInfo(RestUtil).collect()
      tenantInfo.map(f => {
        generateTenantReport(f.id, f.slug, contents)
      })
    }
  }

  def generateTenantReport(tenantId: String, slug: String, contents: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): Unit = {
    implicit val sc = spark.sparkContext
    import spark.implicits._

    val textbookQuery = getDruidQuery(JSONUtils.serialize(config.modelParams.get.getOrElse("textbookQuery","")), tenantId)
    val response = DruidDataFetcher.getDruidData(textbookQuery,true)
    val textbooks = response.map(f=> JSONUtils.deserialize[TextbookDetails](f)).toDF()
    JobLogger.log(s"Textbook count for slug $slug- ${textbooks.count()}",None, Level.INFO)

    val reportDf = contents.join(textbooks, contents.col("collectionId") === textbooks.col("identifier"), "inner").groupBy("contentId","contentName","primaryCategory","identifier",
      "name","board","medium","gradeLevel","subject","programId","createdBy",
      "creator","mimeType","unitIdentifiers", "status","prevStatus")
      .agg(collect_list("acceptedContents").as("acceptedContents"),collect_list("rejectedContents").as("rejectedContents"))

    val finalDf = getContentDetails(reportDf, slug)
    val configMap = JSONUtils.deserialize[Map[String,AnyRef]](JSONUtils.serialize(config.modelParams.get))
    val reportConfig = configMap("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportPath = reportConfig.getOrElse("reportPath","sourcing").asInstanceOf[String]
    val storageConfig = getStorageConfig(config, "")
    saveReportToBlob(finalDf, JSONUtils.serialize(reportConfig), storageConfig, "ContentDetailsReport", reportPath)

    contents.unpersist(true)
    finalDf.unpersist(true)
  }

  def getDruidQuery(query: String, channel: String): DruidQueryModel = {
    val mapQuery = JSONUtils.deserialize[Map[String,AnyRef]](query)
    val filters = JSONUtils.deserialize[List[Map[String, AnyRef]]](JSONUtils.serialize(mapQuery("filters")))
    val channelId = s"""{"channel":"$channel"}""".stripMargin
    val updatedFilters = filters.map(f => {
      f map {
        case ("value","channelId") => "value" -> channelId
        case ("dimension","channel") => "dimension" -> "originData"
        case x => x
      }
    })
    JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(mapQuery.updated("filters",updatedFilters)))
  }

  def getContentDetails(reportDf: DataFrame, slug: String)(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    implicit val sc = spark.sparkContext
    import spark.implicits._
    val contentDf = reportDf.rdd.map(f => {
      val contentStatus = if(f.getAs[Seq[String]](16).contains(f.getString(0))) "Approved" else if(f.getAs[Seq[String]](17).contains(f.getString(0))) "Rejected" else if(null !=f.getString(14) && f.getString(14).equalsIgnoreCase("Draft") && null != f.getString(15) && f.getString(15).equalsIgnoreCase("Live")) "Corrections Pending" else "Pending Approval"
      ContentReport(f.getString(9), f.getString(5), f.getString(6), f.getString(7), f.getString(8),
        f.getString(4),f.getString(3),f.getString(13),f.getString(1),f.getString(0),f.getString(2),f.getString(12),
      contentStatus,f.getString(11),f.getString(10))
    }).toDF().withColumn("slug",lit(slug))

    val programData = spark.read.jdbc(url, programTable, connProperties)
      .select(col("program_id"), col("name").as("programName"))
      .persist(StorageLevel.MEMORY_ONLY)

    val finalDf = programData.join(contentDf, programData.col("program_id") === contentDf.col("programId"), "inner")
      .drop("program_id").persist(StorageLevel.MEMORY_ONLY)
    JobLogger.log(s"Report count for slug $slug- ${finalDf.count()}",None, Level.INFO)

    programData.unpersist(true)
    finalDf
  }

}
