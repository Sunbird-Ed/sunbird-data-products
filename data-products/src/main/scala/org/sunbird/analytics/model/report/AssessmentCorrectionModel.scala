package org.sunbird.analytics.model.report

import com.redislabs.provider.redis._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework._
import org.sunbird.analytics.exhaust.BaseReportsJob

case class SelfAssessData(identifier: String, contentType: String)
case class AssessEvent (contentid: String, batchid: String, courseid: String, userid: String, assessEvent: String)
case class AssessOutputEvent(assessmentTs: Long, batchId: String, courseId: String, userId: String, attemptId: String, contentId: String, events: java.util.List[String]) extends AlgoOutput with Output

object AssessmentCorrectionModel extends IBatchModelTemplate[String,V3Event,AssessOutputEvent,AssessOutputEvent] with Serializable with BaseReportsJob {

  implicit val className: String = "org.sunbird.analytics.model.report.AssessmentCorrectionModel"
  override def name: String = "AssessmentCorrectionModel"

  val redisIndex = AppConf.getConfig("redis.db")
  val redisHost = AppConf.getConfig("redis.host")
  val redisPort = AppConf.getConfig("redis.port")
  val assessEvent = "ASSESS"
  val contentType = "SelfAssess"

  override def preProcess(data: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[V3Event] = {
    data.map(f => JSONUtils.deserialize[V3Event](f)).filter(f => null != f.eid && f.eid.equals(assessEvent))
  }

  override def algorithm(events: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[AssessOutputEvent] = {
    implicit val sqlContext = new SQLContext(sc)

    val groupedDF = getAssessEventData(events)
    val redisData = loadRedisData()

    val joinedDF = groupedDF.join(redisData, groupedDF.col("contentid") === redisData.col("identifier"), "inner")
      .withColumn("attemptId",
        md5(concat(groupedDF.col("contentid"), groupedDF.col("courseid"),
          groupedDF.col("batchid"), groupedDF.col("userid"), lit(System.currentTimeMillis()))))
      .select(groupedDF.col("*"), col("attemptId"))
    joinedDF.show(false)

    val outputData = joinedDF.rdd.map{f =>
      val assessmentTs: Long = System.currentTimeMillis()
      AssessOutputEvent(assessmentTs, f.getString(1), f.getString(2), f.getString(3), f.getString(5), f.getString(0), f.getList(4) )
    }
    outputData
  }

  override def postProcess(events: RDD[AssessOutputEvent], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[AssessOutputEvent] = {
    events
  }

  def loadRedisData()(implicit sc: SparkContext, sqlContext: SQLContext) = {
    import sqlContext.implicits._
    val redisData = sc.fromRedisKV("*")
    val selfAssessFiltered = redisData
      .map(f => (f._1, JSONUtils.deserialize[Map[String, AnyRef]](f._2)))
      .filter(f => contentType.equals(f._2.getOrElse("contentType", "")))
      .map(f => SelfAssessData(f._1, f._2.getOrElse("contentType", "").asInstanceOf[String]))
      .toDF()
    selfAssessFiltered
  }

  def getAssessEventData(events: RDD[V3Event])(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val df = events.map{f =>
      val userId = if (f.actor.`type`.equals("User")) f.actor.id else ""
      val cData = f.context.cdata.getOrElse(List())
      val batchList = cData.filter(f => f.`type`.equalsIgnoreCase("batch")).map(f => f.id)
      val batchId = if (!batchList.isEmpty) batchList.head else ""
      val courseList = cData.filter(f => f.`type`.equalsIgnoreCase("course")).map(f => f.id)
      val courseId = if (!courseList.isEmpty) courseList.head else ""
      val contentId = f.`object`.get.id
      val event = JSONUtils.serialize(f)
      AssessEvent(contentId, batchId, courseId, userId, event)
    }.toDF()
    df.show( false)
    val groupedDF = df.groupBy("contentid","batchid","courseid","userid").agg(collect_list(col("assessEvent")).as("assessEventList"))
    groupedDF.show(false)
    groupedDF
  }

  def isfieldEmpty(data: AssessEvent): Boolean = {
  if (null != data.contentid && !data.contentid.isEmpty && null != data.userid && !data.userid.isEmpty &&
    null != data.courseid && !data.courseid.isEmpty && null != data.batchid && !data.batchid.isEmpty) true
  else false
  }
}
