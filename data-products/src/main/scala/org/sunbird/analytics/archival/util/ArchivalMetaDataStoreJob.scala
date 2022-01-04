package org.sunbird.analytics.archival.util

import java.security.MessageDigest
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
import java.util.Properties
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.col
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.sunbird.analytics.archival.Request

case class ArchivalRequest(request_id: String, batch_id: String, collection_id: String, resource_type: Option[String], job_id: String,
                           var archival_date: Option[Long],var completion_date: Option[Long],var archival_status: String,var deletion_status: String,
                           var blob_url: Option[List[String]],var iteration: Option[Int], request_data: String, var err_message: Option[String])

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
    val encoder = Encoders.product[ArchivalRequest]
    val archivalConfigsDf = spark.read.jdbc(url, requestsTable, connProperties)
      .where(col("job_id") === jobId && col("iteration") < 3)

    val filteredReportConfigDf = if (batchId.isDefined) {
      val filteredArchivalConfig = archivalConfigsDf.filter(col("batch_id").equalTo(batchId.get))
      if (filteredArchivalConfig.count() > 0) filteredArchivalConfig else archivalConfigsDf
    } else archivalConfigsDf

    JobLogger.log("fetched records count: " + filteredReportConfigDf.count(), None, INFO)
    val requests = filteredReportConfigDf.as[ArchivalRequest](encoder).collect()
    requests
  }

  def getRequestID(jobId: String, collectionId: String, batchId: String, partitionCols: List[Int]): String = {
    val requestComb = s"$jobId:$collectionId:$batchId:" + partitionCols.mkString(":")
    MessageDigest.getInstance("MD5").digest(requestComb.getBytes).map("%02X".format(_)).mkString
  }

  def getRequest(jobId: String, collectionId: String, batchId: String, partitionCols: List[Int]): ArchivalRequest = {
    val requestId = getRequestID(jobId, collectionId, batchId, partitionCols)
    val archivalRequest = s"""select * from $requestsTable where request_id = '$requestId' limit 1"""
    val pstmt: PreparedStatement = dbc.prepareStatement(archivalRequest);
    val resultSet = pstmt.executeQuery()

    if (resultSet.next()) getArchivalRequest(resultSet) else null
  }

  private def getArchivalRequest(resultSet: ResultSet): ArchivalRequest = {
    ArchivalRequest(
      resultSet.getString("request_id"),
      resultSet.getString("batch_id"),
      resultSet.getString("collection_id"),
      Option(resultSet.getString("resource_type")),
      resultSet.getString("job_id"),
      Option(resultSet.getTimestamp("archival_date").getTime),
      if (resultSet.getTimestamp("completion_date") != null) Option(resultSet.getTimestamp("completion_date").getTime) else None,
      resultSet.getString("archival_status"),
      resultSet.getString("deletion_status"),
      if (resultSet.getArray("blob_url") != null) Option(resultSet.getArray("blob_url").getArray().asInstanceOf[Array[String]].toList) else None,
      Option(resultSet.getInt("iteration")),
      resultSet.getString("request_data"),
      Option(resultSet.getString("err_message"))
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

  def createRequest(request: ArchivalRequest) = {
    val insertQry = s"INSERT INTO $requestsTable (request_id, batch_id, collection_id, resource_type, job_id, archival_date, completion_date, archival_status, " +
      s"deletion_status, blob_url, iteration, request_data, err_message) VALUES (?,?,?,?,?,?,?,?,?,?,?,?::json,?)"
    val pstmt: PreparedStatement = dbc.prepareStatement(insertQry);
    val request_data = JSONUtils.deserialize[Map[String, AnyRef]](request.request_data)
    val requestId = getRequestID(request.job_id, request.collection_id, request.batch_id, List(request_data("year").asInstanceOf[Int], request_data("week").asInstanceOf[Int]))
    pstmt.setString(1, requestId);
    pstmt.setString(2, request.batch_id);
    pstmt.setString(3, request.collection_id);
    pstmt.setString(4, request.resource_type.getOrElse("assessment"));
    pstmt.setString(5, request.job_id);
    pstmt.setTimestamp(6, if (request.archival_date.isDefined) new Timestamp(request.archival_date.get) else null);
    pstmt.setTimestamp(7, if (request.completion_date.isDefined) new Timestamp(request.completion_date.get) else null);
    pstmt.setString(8, request.archival_status);
    pstmt.setString(9, request.deletion_status);
    val blobURLs = request.blob_url.getOrElse(List()).toArray.asInstanceOf[Array[Object]];
    pstmt.setArray(10, dbc.createArrayOf("text", blobURLs))
    pstmt.setInt(11, request.iteration.getOrElse(0))
    pstmt.setString(12, request.request_data)
    pstmt.setString(13, StringUtils.abbreviate(request.err_message.getOrElse(""), 300));

    pstmt.execute()
  }

  def upsertRequest(request: ArchivalRequest): Unit = {
    if (request.request_id.isEmpty) {
      createRequest(request)
    } else {
      updateRequest(request)
    }
  }

  def updateRequest(request: ArchivalRequest): Unit = {
    val updateQry = s"UPDATE $requestsTable SET blob_url=?, iteration = ?, archival_date=?, completion_date=?, " +
      s"archival_status=?, deletion_status=?, err_message=? WHERE request_id=?";
    val pstmt: PreparedStatement = dbc.prepareStatement(updateQry)

    val blobURLs = request.blob_url.getOrElse(List()).toArray.asInstanceOf[Array[Object]];
    pstmt.setArray(1, dbc.createArrayOf("text", blobURLs))
    pstmt.setInt(2, request.iteration.get);
    pstmt.setTimestamp(3, if (request.archival_date.isDefined) new Timestamp(request.archival_date.get) else null);
    pstmt.setTimestamp(4, if (request.completion_date.isDefined) new Timestamp(request.completion_date.get) else null);
    pstmt.setString(5, request.archival_status);
    pstmt.setString(6, request.deletion_status);
    pstmt.setString(7, request.err_message.getOrElse(""));
    pstmt.setString(8, request.request_id);

    pstmt.execute()
  }

  def markArchivalRequestAsSuccess(request: ArchivalRequest, requestConfig: Request): ArchivalRequest = {
    request.archival_status = "SUCCESS";
    request.archival_date = Option(System.currentTimeMillis())
    request
  }

  def markDeletionRequestAsSuccess(request: ArchivalRequest, requestConfig: Request): ArchivalRequest = {
    request.deletion_status = "SUCCESS";
    request.completion_date = Option(System.currentTimeMillis())
    request
  }

}
