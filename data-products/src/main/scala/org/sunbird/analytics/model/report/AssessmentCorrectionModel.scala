package org.sunbird.analytics.model.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lower, _}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.sunbird.analytics.job.report.DruidOutput

case class SelfAssessData(identifier: String, contentType: String)
case class AssessEvent (contentid: String, attemptId: String, courseid: String, userid: String, assessEvent: String)
case class AssessOutputEvent(assessmentTs: Long, batchId: String, courseId: String, userId: String, attemptId: String, contentId: String, events: java.util.List[String]) extends AlgoOutput with Output

object AssessmentCorrectionModel extends IBatchModelTemplate[String,V3Event,AssessOutputEvent,AssessOutputEvent] with Serializable with BaseReportsJob {

  implicit val className: String = "org.sunbird.analytics.model.report.AssessmentCorrectionModel"
  override def name: String = "AssessmentCorrectionModel"

  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  val cassandraFormat = "org.apache.spark.sql.cassandra";

  val assessEvent = "ASSESS"
  val contentType = "SelfAssess"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[V3Event] = {
    data.map(f => JSONUtils.deserialize[V3Event](f)).filter(f => null != f.eid && f.eid.equals(assessEvent))
  }

  override def algorithm(events: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[AssessOutputEvent] = {
    implicit val sqlContext = new SQLContext(sc)

    val assessEventDF = getAssessEventData(events)
    val druidData = loadDruidData(config)

    val joinedDF = assessEventDF.join(druidData, assessEventDF.col("contentid") === druidData.col("identifier"), "inner")
      .select(assessEventDF.col("*"))

    val outputData = joinedDF.rdd.map{f =>
      val assessmentTs: Long = System.currentTimeMillis()
      AssessOutputEvent(assessmentTs, f.getString(4), f.getString(1), f.getString(2), f.getString(3), f.getString(0), f.getList(5) )
    }
    outputData
  }

  override def postProcess(events: RDD[AssessOutputEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[AssessOutputEvent] = {
    events
  }

  def loadDruidData(config: Map[String, AnyRef])(implicit sc: SparkContext, sqlContext: SQLContext, fc: FrameworkContext): DataFrame = {
    import sqlContext.implicits._
    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.get("modelParams").get.asInstanceOf[Map[String, AnyRef]].get("druidConfig").get))
    val druidResponse = DruidDataFetcher.getDruidData(druidConfig)
    val dataDF = druidResponse.map(f => JSONUtils.deserialize[DruidOutput](f)).toDF.select("identifier")
    dataDF
  }

  def getAssessEventData(events: RDD[V3Event])(implicit sqlContext: SQLContext, sc: SparkContext): DataFrame = {
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
    val userEnrollmentData = getUserEnrollData()
    val df = assessEvent.join(userEnrollmentData, Seq("courseid", "userid"))
      .groupBy("contentid", "courseid", "userid", "attemptId", "batchid")
      .agg(collect_list(col("assessEvent")).as("assessEventList"))
    df
  }

  def getUserEnrollData()(implicit sqlContext: SQLContext): DataFrame = {
    implicit val spark = sqlContext.sparkSession
    loadData(userEnrolmentDBSettings,cassandraFormat, new StructType())
      .where(lower(col("active")).equalTo("true") && col("enrolleddate").isNotNull)
      .select("userid", "courseid", "batchid")
  }
}