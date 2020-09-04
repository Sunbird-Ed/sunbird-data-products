package org.sunbird.analytics.job.report

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework._
import org.sunbird.analytics.job.report.CourseReport.fetchData


trait ReportOnDemandModelTemplate extends ReportOnDemandModel {

  /**
   * Override and implement the data product execute method,
   * 1. filterReports
   * 2. generateReports
   * 3. saveReports
   */

  override def execute(reportParams: Option[Map[String, AnyRef]])(implicit spark: SparkSession, fc: FrameworkContext): Unit = {
    println("Executing the report job...")
    val config = reportParams.getOrElse(Map[String, AnyRef]())

    val reportConfigList = getReportConfigs(config.getOrElse("jobId", "").asInstanceOf[String])

    val filteredReports = filterReports(reportConfigList, config)

    val generatedReports = generateReports(filteredReports, config, fetchData)

    //val savedReports = saveReports(generatedReports, config)

    // saveReportLocations(savedReports)


  }

  /**
   * Method will get the list the active on demand reports from table
   *
   * @return
   */
  def getReportConfigs(jobId: String): DataFrame = ???



  /**
   * Method will save the list of report blob paths for each request id
   *
   * @return
   */
  def saveReportLocations(requestedReports: Map[String, List[String]]): DataFrame = ???

  /**
   * filter Reports steps before generating Report. Few pre-process steps are
   * 1. Combine or filter the report configs an
   * 2. Join or fetch Data from Tables
   */
  def filterReports(reportConfigs: DataFrame, config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame

  /**
   * Method which will generate report
   * Input : List of Filtered Ids to generate Report
   * Output : List of Files to be saved per request
   */
  def generateReports(filteredReports: DataFrame, config: Map[String, AnyRef], fetchData: (SparkSession, Map[String, String], String, Option[StructType], Option[Seq[String]]) => DataFrame)(implicit spark: SparkSession, fc: FrameworkContext): Unit

  /**
   * .
   * 1. Saving Reports to Blob
   * 2. Generate Metrics
   * 3. Return Map list of blobs to RequestIds as per the request
   */
  def saveReports(generatedreports: DataFrame, config: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext): DataFrame

}