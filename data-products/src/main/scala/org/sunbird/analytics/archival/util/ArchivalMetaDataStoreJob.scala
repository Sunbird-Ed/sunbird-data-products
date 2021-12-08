package org.sunbird.analytics.archival.util

import java.security.MessageDigest
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
import java.util.Properties
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.job.batch.VideoStreamingJob.JobRequest
import org.sunbird.analytics.archival.Request

case class ArchivalRequest(request_id: String, batch_id: String, collection_id: String, resource_type: Option[String], job_id: String,
                           var archival_date: Option[Long],var completion_date: Option[Long],var archival_status: String,var deletion_status: String,
                           blob_url: Option[List[String]],var iteration: Option[Int], request_data: Option[String],var err_message: Option[String])

trait ArchivalMetaDataStoreJob {

  implicit val className: String = getClassName;
  val connProperties: Properties = CommonUtil.getPostgresConnectionProps()
  val db: String = AppConf.getConfig("postgres.db")
  val url: String = AppConf.getConfig("postgres.url") + s"$db"
  val requestsTable: String = AppConf.getConfig("postgres.table.archival_request")
  val dbc: Connection = DriverManager.getConnection(url, connProperties.getProperty("user"), connProperties.getProperty("password"));
  dbc.setAutoCommit(true);

  def getClassName(): String;

  def cleanUp() {
    dbc.close();
  }

  def getRequests(jobId: String, batchId: Option[String])(implicit spark: SparkSession, fc: FrameworkContext): Array[ArchivalRequest] = {
    println("jobid: " + jobId + " batchid: " + batchId)
    val encoder = Encoders.product[ArchivalRequest]
    val archivalConfigsDf = spark.read.jdbc(url, requestsTable, connProperties)
      .where(col("job_id") === jobId && col("iteration") < 3)
    println("archivalConfigDF:")
    archivalConfigsDf.show(false)

    val filteredReportConfigDf = if (batchId.isDefined) {
      val filteredArchivalConfig = archivalConfigsDf.filter(col("batch_id").equalTo(batchId.get))
      if (filteredArchivalConfig.count() > 0) filteredArchivalConfig else archivalConfigsDf
    } else archivalConfigsDf
    println("filteredtReportCOnfig: ")
    filteredReportConfigDf.show(false)
    JobLogger.log("fetched records count" + filteredReportConfigDf.count(), None, INFO)
    val requests = filteredReportConfigDf.as[ArchivalRequest](encoder).collect()
    requests
  }

  def getRequestID(collectionId: String, batchId: String, year: Int, week: Int): String = {
    val requestComb = s"$collectionId:$batchId:$year:$week"
    MessageDigest.getInstance("MD5").digest(requestComb.getBytes).map("%02X".format(_)).mkString
  }

  def getRequest(collectionId: String, batchId: String, year: Int, week: Int): ArchivalRequest = {
    val requestId = getRequestID(collectionId, batchId, year, week)
    val archivalRequest = s"""select * from $requestsTable where request_id = $requestId"""
    val pstmt: PreparedStatement = dbc.prepareStatement(archivalRequest);
    val resultSet = pstmt.executeQuery()

    getArchivalRequest(resultSet)
  }

  private def getArchivalRequest(resultSet: ResultSet): ArchivalRequest = {
    ArchivalRequest(
      resultSet.getString("request_id"),
      resultSet.getString("batch_id"),
      resultSet.getString("collection_id"),
      Some(resultSet.getString("resource_type")),
      resultSet.getString("job_id"),
      Some(resultSet.getLong("archival_date")),
      Some(resultSet.getLong("completion_date")),
      resultSet.getString("archival_status"),
      resultSet.getString("deletion_status"),
      Some(resultSet.getArray("blob_url").asInstanceOf[List[String]]),
      Some(resultSet.getInt("iteration")),
      Some(resultSet.getString("request_data")),
      Some(resultSet.getString("err_message"))
    )
  }

  def markArchivalRequestAsFailed(request: ArchivalRequest, failedMsg: String): ArchivalRequest = {
    request.archival_status = "FAILED";
    request.archival_date = Option(System.currentTimeMillis());
    request.iteration = Option(request.iteration.getOrElse(0) + 1);
    request.err_message = Option(failedMsg);
    request
  }

  def markDeletionRequestAsFailed(request: ArchivalRequest, failedMsg: String): ArchivalRequest = {
    request.deletion_status = "FAILED";
    request.archival_date = Option(System.currentTimeMillis());
    request.iteration = Option(request.iteration.getOrElse(0) + 1);
    request.err_message = Option(failedMsg);
    request
  }

  def markRequestAsSuccess(request: ArchivalRequest, requestConfig: Request): Boolean = {
    val insertQry = s"INSERT INTO $requestsTable (request_id, batch_id, collection_id, resource_type, job_id, archival_date, completion_date, archival_status, " +
      s"deletion_status, blob_url, iteration, request_data, err_message) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
    val pstmt: PreparedStatement = dbc.prepareStatement(insertQry);
    pstmt.setString(1, request.request_id);
    pstmt.setString(2, requestConfig.batchId.getOrElse(""));
    pstmt.setString(3, requestConfig.collectionId.getOrElse(""));
    pstmt.setString(4, request.resource_type.getOrElse("assessment"));
    pstmt.setString(5, request.job_id);
    pstmt.setTimestamp(6, if (request.archival_date.isDefined) new Timestamp(request.archival_date.get) else null);
    pstmt.setTimestamp(7, if (request.completion_date.isDefined) new Timestamp(request.completion_date.get) else null);
    pstmt.setString(8, request.archival_status);
    pstmt.setString(9, request.deletion_status);
    val blobURLs = request.blob_url.getOrElse(List()).toArray.asInstanceOf[Array[Object]];
    pstmt.setArray(10, dbc.createArrayOf("text", blobURLs))
    pstmt.setInt(11, request.iteration.getOrElse(0))
    pstmt.setString(12, request.request_data.getOrElse("[]"))
    pstmt.setString(13, StringUtils.abbreviate(request.err_message.getOrElse(""), 300));

    pstmt.execute()
  }

}
