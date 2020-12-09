package org.sunbird.analytics.audit

import org.ekstep.analytics.framework.IJob
import org.sunbird.analytics.exhaust.BaseReportsJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.apache.spark.sql.{ DataFrame, Encoders, SparkSession }
import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Level.INFO
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import com.datastax.spark.connector._
import org.apache.spark.sql.Row
import java.util.UUID
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher

object CollectionReconciliationJob extends optional.Application with IJob with BaseReportsJob {

  implicit val className: String = "org.sunbird.analytics.audit.CollectionReconciliationJob"
  val jobName = "CollectionReconciliationJob"
  val cassandraFormat = "org.apache.spark.sql.cassandra";

  private val userEnrolmentDBSettings = Map("table" -> "user_enrolments", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  private val userEnrolmentTempDBSettings = Map("table" -> "user_enrolments_temp", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  private val collectionBatchDBSettings = Map("table" -> "course_batch", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  private val contentConsumptionDBSettings = Map("table" -> "user_content_consumption", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  private val contentHierarchyDBSettings = Map("table" -> "content_hierarchy", "keyspace" -> AppConf.getConfig("sunbird.content.hierarchy.keyspace"), "cluster" -> "ContentCluster");
  case class UserEnrolment(userid: String, courseid: String, batchid: String)

  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {

    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    init()
    try {
      val res = CommonUtil.time(execute());
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map("timeTaken" -> res._1)))
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }
  }

  def init()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) {
    spark.setCassandraConf("UserCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.user.cluster.host")))
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    spark.setCassandraConf("ContentCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.content.cluster.host")))
  }

  def execute()(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig) = {
    
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    implicit val dryRunEnabled:Boolean = modelParams.getOrElse("mode", "dryrun").asInstanceOf[String].equals("dryrun")
    logTime(reconcileProgressUpdates(), "Time taken to reconcile progress updates");
    logTime(reconcileMissingCertsAndEnrolmentDates(modelParams), "Time taken to reconcile progress updates");
  }

  def reconcileProgressUpdates()(implicit spark: SparkSession, fc: FrameworkContext, dryRunEnabled: Boolean) = {
    val enrolmentDF = loadData(userEnrolmentDBSettings, cassandraFormat, new StructType());
    val notcompletedEnrolments = enrolmentDF.filter(col("completedon").isNull).cache();
    val distinctCourses = notcompletedEnrolments.select("courseid").distinct()
    val coursesDF = getCollectionLeafNodes();
    val joinedCoursesDF = distinctCourses.join(coursesDF, "courseid")

    val nonCompleteEnrolmentsDF = notcompletedEnrolments.select("userid", "batchid", "courseid", "contentstatus", "enrolleddate")
    val enrolmentCourseJoinedDF = nonCompleteEnrolmentsDF.join(joinedCoursesDF, "courseid")
      .withColumn("contentstatus", updateContentStatusMap(col("contentstatus"), col("leafnodes")))
      .withColumn("contentstatusupdated", contentStatusUpdated(col("contentstatus"), col("leafnodes")))
      .withColumn("completed", completed(col("contentstatus"), col("leafnodescount"))).cache();

    val contentStatusMapUpdCount = enrolmentCourseJoinedDF.filter(col("contentstatusupdated") === "Yes").count();
    val missingCompletionDateCount = enrolmentCourseJoinedDF.filter(col("completed") === "Yes").count();

    if (contentStatusMapUpdCount > 0) {
      updateContentStatus(enrolmentCourseJoinedDF)
    }

    if (missingCompletionDateCount > 0) {
      updateCompletions(enrolmentCourseJoinedDF)
    }
    notcompletedEnrolments.unpersist(true);
    enrolmentCourseJoinedDF.unpersist(true);
  }
  
  def reconcileMissingCertsAndEnrolmentDates(modelParams: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext, dryRunEnabled: Boolean) = {

    implicit val sc = spark.sparkContext;
    val enrolmentDF = loadData(userEnrolmentDBSettings, cassandraFormat, new StructType()).cache();
    val courseBatchDF = loadData(collectionBatchDBSettings, cassandraFormat, new StructType()).select("batchid", "enddate");
    val courseBatchMinDF = courseBatchDF.withColumn("hasSVGCertificate", hasSVGCertificate(col("cert_templates"))).select("batchid", "name", "hasSVGCertificate", "startdate", "enddate").cache();
    val joinedDF = enrolmentDF.join(courseBatchMinDF, "batchid").filter(col("completedon").isNotNull).withColumn("certificatestatus", when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0, "Issued")
      .when(col("issued_certificates").isNotNull && size(col("issued_certificates").cast("array<map<string, string>>")) > 0, "Issued").otherwise("")).cache();

    val finalDF = joinedDF.select("batchid", "userid", "courseid", "hasSVGCertificate", "certificatestatus").filter(col("hasSVGCertificate") === "Yes").filter(col("certificatestatus") =!= "Issued").select("batchid", "userid", "courseid")
    val certIssueRDD = finalDF.rdd.map(f => certEvent(f))
    if(dryRunEnabled) {
      OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "/mount/data/analytics/cert_events.json")), certIssueRDD)
    } else {
      OutputDispatcher.dispatch(Dispatcher("kafka", Map("brokerList" -> modelParams.getOrElse("brokerList", ""), "topic" -> modelParams.getOrElse("topic", ""))), certIssueRDD)
    }
    joinedDF.unpersist(true);
    
    // Fix blank enrolment dates
    val blankEnrolmentDatesDF = enrolmentDF.filter(col("enrolleddate").isNull).cache();
    val blankEnrolmentCount = blankEnrolmentDatesDF.count()
    if(blankEnrolmentCount > 0) {
      updateEnrolmentDates(blankEnrolmentDatesDF, courseBatchMinDF);
    }
    
  }
  
  private def certEvent(row: Row): String = {
    val batchid = row.getString(0);
    val userid = row.getString(1);
    val courseid = row.getString(2);
    val eventMap = Map(
        "eid" -> "BE_JOB_REQUEST",
        "ets" -> System.currentTimeMillis(),
        "mid" -> ("LP-" + UUID.randomUUID().toString()),
        "actor" -> Map("id" -> "Course Certificate Generator","type" -> "System"),
        "context" -> Map("pdata" -> Map("ver" -> "1.0","id" -> "org.sunbird.platform")),
        "object" -> Map("id" -> (batchid + "_" + courseid),"type" -> "CourseCertificateGeneration"),
        "edata" -> Map("userIds" -> Array(userid),"action" -> "issue-certificate","iteration" -> 1,"batchId" -> batchid,"reIssue" -> false,"courseId" -> courseid)
    )
    JSONUtils.serialize(eventMap);
  }

  def updateContentStatus(enrolmentCourseJoinedDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, dryRunEnabled: Boolean) = {
    enrolmentCourseJoinedDF.select("userid", "batchid", "courseid", "contentstatus").write.format(cassandraFormat).options(if(dryRunEnabled) userEnrolmentTempDBSettings else userEnrolmentDBSettings).mode("APPEND").save()
  }

  def updateCompletions(enrolmentCourseJoinedDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, dryRunEnabled: Boolean) = {

    import spark.implicits._
    val courseBatchDF = loadData(collectionBatchDBSettings, cassandraFormat, new StructType()).select("batchid", "enddate");
    val enrolmentDF = enrolmentCourseJoinedDF.filter(col("completed") === "Yes").select("userid", "courseid", "batchid");
    val enrolmentRDD = enrolmentDF.rdd.repartition(30).map(f => UserEnrolment(f.getString(0), f.getString(1), f.getString(2)))

    val joinedRdd = enrolmentRDD.joinWithCassandraTable("sunbird_courses", "user_content_consumption", SomeColumns("contentid", "status", "lastcompletedtime"), SomeColumns("userid", "batchid", "courseid"))
    val finalRDD = joinedRdd.filter(f => f._2.getInt(1) == 2).map(f => (f._1.userid, f._1.batchid, f._1.courseid, f._2.getString(2)))
    val enrolmentJoinedDF = finalRDD.toDF().withColumnRenamed("_1", "userid").withColumnRenamed("_2", "batchid").withColumnRenamed("_3", "courseid").withColumnRenamed("_4", "lastcompletedtime")
    val enrolmentWithRevisedDtDF = enrolmentJoinedDF.groupBy("userid", "batchid", "courseid").agg(max(col("lastcompletedtime")).alias("lastreviseddate")).withColumn("lastrevisedon", unix_timestamp(col("lastreviseddate"), "yyyy-MM-dd HH:mm:ss:SSS"))
    enrolmentWithRevisedDtDF.join(courseBatchDF, "batchid")
      .withColumn("endedon", unix_timestamp(col("enddate"), "yyyy-MM-dd"))
      .withColumn("completedon", when(col("endedon").isNotNull, when(col("endedon") < col("lastrevisedon"), col("endedon")).otherwise(col("lastrevisedon"))).otherwise(col("lastrevisedon")))
      .withColumn("status", lit(2))
      .select("userid", "courseid", "batchid", "status", "completedon")
      .write.format(cassandraFormat).options(if(dryRunEnabled) userEnrolmentTempDBSettings else userEnrolmentDBSettings).mode("APPEND").save()
  }
  
  def updateEnrolmentDates(blankEnrolmentDatesDF: DataFrame, courseBatchMinDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, dryRunEnabled: Boolean) = {

    import spark.implicits._
    val enrolmentDF = blankEnrolmentDatesDF.select("userid", "courseid", "batchid");
    val enrolmentRDD = enrolmentDF.rdd.repartition(30).map(f => UserEnrolment(f.getString(0), f.getString(1), f.getString(2)))
    val joinedRdd = enrolmentRDD.joinWithCassandraTable("sunbird_courses", "user_content_consumption", SomeColumns("contentid", "status", "lastcompletedtime"), SomeColumns("userid", "batchid", "courseid")).cache();
    val finalRDD = joinedRdd.map(f => (f._1.userid, f._1.batchid, f._1.courseid, f._2.getStringOption(2).getOrElse(null))).filter(f => f._4 != null)
    val enrolmentJoinedDF = finalRDD.toDF().withColumnRenamed("_1", "userid").withColumnRenamed("_2", "batchid").withColumnRenamed("_3", "courseid").withColumnRenamed("_4", "lastcompletedtime")
    val enrolmentWithRevisedDtDF = enrolmentJoinedDF.groupBy("userid", "batchid", "courseid").agg(min(col("lastcompletedtime")).alias("lastreviseddate")).withColumn("lastrevisedon", unix_timestamp(col("lastreviseddate"), "yyyy-MM-dd HH:mm:ss:SSS"))
    
    val finalEnrolmentDF = enrolmentDF.join(enrolmentWithRevisedDtDF, Seq("userid", "batchid", "courseid"), "left_outer")
    finalEnrolmentDF.join(courseBatchMinDF, "batchid")
      .withColumn("startedon", unix_timestamp(col("startdate"), "yyyy-MM-dd"))
      .withColumn("startedtimestamp", when(col("lastrevisedon").isNotNull, when(col("startedon") > col("lastrevisedon"), col("startedon")).otherwise(col("lastrevisedon"))).otherwise(col("startedon")))
      .withColumn("enrolleddate", from_unixtime(col("startedtimestamp")))
      .select("userid", "courseid", "batchid", "enrolleddate")
      .write.format(cassandraFormat).options(if(dryRunEnabled) userEnrolmentTempDBSettings else userEnrolmentDBSettings).mode("APPEND").save()
  }

  def getCollectionLeafNodes()(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    val coursesDF = loadData(contentHierarchyDBSettings, cassandraFormat, new StructType()).select("identifier", "hierarchy");
    import spark.implicits._
    val courseLeafNodeCountsDF = coursesDF.rdd.map(row => {
      val hierarchy = JSONUtils.deserialize[Map[String, AnyRef]](row.getString(1));
      (row.getString(0), hierarchy.getOrElse("leafNodes", List[String]()).asInstanceOf[List[String]], hierarchy.getOrElse("leafNodesCount", 0).asInstanceOf[Int])
    }).toDF();
    courseLeafNodeCountsDF.withColumnRenamed("_1", "courseid").withColumnRenamed("_2", "leafnodes").withColumnRenamed("_3", "leafnodescount");
  }

  def updateContentStatusMapFunction(statusMap: Map[String, Int], leafNodes: WrappedArray[String]): Map[String, Int] = {
    try {
      if (statusMap != null && leafNodes != null) {
        statusMap.filterKeys(p => leafNodes.contains(p))
      } else statusMap
    } catch {
      case ex: Exception =>
        ex.printStackTrace();
        statusMap
    }
  }

  val updateContentStatusMap = udf[Map[String, Int], Map[String, Int], WrappedArray[String]](updateContentStatusMapFunction)

  def contentStatusUpdatedFunction(statusMap: Map[String, Int], leafNodes: WrappedArray[String]): String = {
    try {
      if (statusMap != null && leafNodes != null) {
        val currentSize = statusMap.size;
        val newSize = statusMap.filterKeys(p => leafNodes.contains(p)).size
        if (currentSize != newSize) "Yes" else "No"
      } else "No"
    } catch {
      case ex: Exception =>
        ex.printStackTrace();
        "No"
    }
  }

  val contentStatusUpdated = udf[String, Map[String, Int], WrappedArray[String]](contentStatusUpdatedFunction)

  def completedFunction(statusMap: Map[String, Int], leafNodesCount: Int): String = {
    try {
      if (statusMap != null) {
        val completedCount = statusMap.filter(p => p._2 == 2).size
        if (completedCount == leafNodesCount) "Yes" else "No"
      } else "No"
    } catch {
      case ex: Exception =>
        ex.printStackTrace();
        "No"
    }
  }

  val completed = udf[String, Map[String, Int], Int](completedFunction)

  def hasSVGCertificateFunction(templates: Map[String, Map[String, String]]): String = {
    try {
      val templateStr = JSONUtils.serialize(templates);
      if (templateStr.toLowerCase().contains(".svg")) "Yes" else "No"
    } catch {
      case ex: Exception =>
        ex.printStackTrace();
        "No"
    }
  }

  val hasSVGCertificate = udf[String, Map[String, Map[String, String]]](hasSVGCertificateFunction)

  def logTime[R](block: => R, message: String): R = {
    val res = CommonUtil.time(block);
    JobLogger.log(message, Some(Map("timeTaken" -> res._1)), INFO)
    res._2
  }

}