package org.sunbird.analytics.model.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lower, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.FileDispatcher
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.analytics.job.report.DruidOutput

import scala.collection.JavaConverters._
import scala.collection.mutable
case class SelfAssessData(identifier: String, contentType: String)
case class AssessEvent (contentid: String, attemptId: String, courseid: String, userid: String, assessEvent: String)
@scala.beans.BeanInfo
case class AssessOutputEvent(assessmentTs: Long, batchId: String, courseId: String, userId: String, attemptId: String, contentId: String, events: mutable.Buffer[V3Event]) extends Output with AlgoOutput with scala.Product with scala.Serializable

object AssessmentCorrectionModel extends IBatchModelTemplate[String,V3Event,AssessOutputEvent,AssessOutputEvent] with Serializable with BaseReportsJob {

  implicit val className: String = "org.sunbird.analytics.model.report.AssessmentCorrectionModel"
  override def name: String = "AssessmentCorrectionModel"

  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  val cassandraFormat = "org.apache.spark.sql.cassandra";
  val fileName = AppConf.getConfig("assessment.output.path")
  val assessEvent = "ASSESS"
  val contentType = "SelfAssess"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[V3Event] = {
    JobLogger.log(s"Total input events from backup: ${data.count()}", None, Level.INFO)
    data.map(f => JSONUtils.deserialize[V3Event](f)).filter(f => null != f.eid && f.eid.equals(assessEvent) && null != f.`object`.get.rollup.getOrElse(RollUp("", "", "", "")).l1 && !f.`object`.get.rollup.getOrElse(RollUp("", "", "", "")).l1.isEmpty)
  }

  override def algorithm(events: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[AssessOutputEvent] = {
    val res = CommonUtil.time(algorithProcess(events,config))

    JobLogger.log(s"Time take to get SelfAssess data: ${res._1}", None, Level.INFO)
    res._2
  }

  override def postProcess(events: RDD[AssessOutputEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[AssessOutputEvent] = {
    JobLogger.log(s"Total output events: ${events.count()}", None, Level.INFO)
    dispatchAssessData(events)
    events
  }

  def dispatchAssessData(events: RDD[AssessOutputEvent])(implicit sc: SparkContext, fc: FrameworkContext): Unit ={
    val filterDF = events.map(f => JSONUtils.serialize(f)).filter(f => f.getBytes.length.toLong >= 1000000.asInstanceOf[Long])
    if (filterDF.count() > 0) {
      FileDispatcher.dispatch(Map("file" -> fileName.asInstanceOf[AnyRef]), filterDF)
    }
  }

  def algorithProcess(events: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[AssessOutputEvent] = {
    implicit val sqlContext = new SQLContext(sc)
    val assessEventDF = getAssessEventData(events, config)
    val druidData = loadDruidData(config)

    val joinedDF = assessEventDF.join(druidData, assessEventDF.col("contentid") === druidData.col("identifier"), "inner")
      .select(assessEventDF.col("*"))

    joinedDF.rdd.map{f =>
      val assessmentTs: Long = System.currentTimeMillis()
      val eventList = f.getList(5).asInstanceOf[java.util.List[String]].asScala.map(f => JSONUtils.deserialize[V3Event](f))
      AssessOutputEvent(assessmentTs, f.getString(4), f.getString(1), f.getString(2), f.getString(3), f.getString(0), eventList)
    }
  }

  def loadDruidData(config: Map[String, AnyRef])(implicit sc: SparkContext, sqlContext: SQLContext, fc: FrameworkContext): DataFrame = {
    import sqlContext.implicits._
    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.get("druidConfig").get))
    val druidResponse = DruidDataFetcher.getDruidData(druidConfig)
    val dataDF = druidResponse.map(f => JSONUtils.deserialize[DruidOutput](f)).toDF.select("identifier")
    dataDF
  }

  def getAssessEventData(events: RDD[V3Event], config: Map[String, AnyRef])(implicit sqlContext: SQLContext, sc: SparkContext): DataFrame = {
    import sqlContext.implicits._
    val assessEvent = events.map { f =>
      val userId = if (f.actor.`type`.equals("User")) f.actor.id else "" // UserID
    val cData = f.context.cdata.getOrElse(List())
    val attemptIdList = cData.filter(f => f.`type`.equalsIgnoreCase("AttemptId")).map(f => f.id)
    val attemptId = if (!attemptIdList.isEmpty) attemptIdList.head else ""  // AttempId
    val courseId = f.`object`.get.rollup.getOrElse(RollUp("", "", "", "")).l1 // CourseId
    val contentId = f.`object`.get.id // ContentId
    val event = JSONUtils.serialize(f)
      AssessEvent(contentId, attemptId, courseId, userId, event)
    }.toDF

    //Filter batchIds from config
    val batchIds: List[String] = config.getOrElse("batchId", List()).asInstanceOf[List[String]]
    val userEnrollmentData = getUserEnrollData(batchIds)

    assessEvent.join(userEnrollmentData, Seq("courseid", "userid"))
      .groupBy("contentid", "courseid", "userid", "attemptId", "batchid")
      .agg(collect_list(col("assessEvent")).as("assessEventList"))
      .withColumn("AttemptId_resolved",
        when(col("attemptId").equalTo(""), md5(concat(col("contentid"), col("courseid"), col("userid"), col("batchid")))).otherwise(col("attemptId")))
      .select(
        col("contentid"),
        col("courseid"),
        col("userid"),
        col("AttemptId_resolved").as("attemptId"),
        col("batchid"),
        col("assessEventList")
      )
  }

  def getUserEnrollData(batchIds: List[String])(implicit sqlContext: SQLContext): DataFrame = {
    implicit val spark = sqlContext.sparkSession
    if(batchIds.size == 0) {
      JobLogger.log("No batchid provided", None, Level.INFO)
      loadData(userEnrolmentDBSettings,cassandraFormat, new StructType())
        .where(lower(col("active")).equalTo("true") && col("enrolleddate").isNotNull)
        .select("userid", "courseid", "batchid")
    } else {
      JobLogger.log(s"Filtering for batches: ${batchIds}", None, Level.INFO)
      loadData(userEnrolmentDBSettings,cassandraFormat, new StructType())
        .where(lower(col("active")).equalTo("true") && col("enrolleddate").isNotNull)
        .filter(col("batchid").isin(batchIds: _*))
        .select("userid", "courseid", "batchid")
    }
  }
}
