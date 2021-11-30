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
import org.sunbird.analytics.exhaust.collection.UDFUtils

object CollectionReconciliationJob extends optional.Application with IJob with BaseReportsJob {

  // $COVERAGE-OFF$ Disabling scoverage because the job will be deprecated
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
    JobLogger.start(s"$jobName started executing - version 7", Option(Map("config" -> config, "model" -> jobName)))
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
    
    val preAuditEvent = logTime(audit(), "Time taken to execute pre audit check"); // Pre Audit Check
    JobLogger.log("Pre Audit Check", Option(preAuditEvent), INFO)
    
    val progressMetrics = logTime(reconcileProgressUpdates(), "Time taken to reconcile progress updates");
    JobLogger.log("Reconcile Missing Completion Dates", Option(progressMetrics), INFO)
    
    val certMetrics = logTime(reconcileMissingCertsAndEnrolmentDates(modelParams), "Time taken to reconcile enrollment dates");
    JobLogger.log("Reconcile Certificates and Enrolments", Option(certMetrics), INFO)
    
    val postAuditEvent = logTime(audit(), "Time taken to execute post audit check"); // Post Audit Check
    JobLogger.log("Post Audit Check", Option(postAuditEvent), INFO)
    
  }

  def audit()(implicit spark: SparkSession, fc: FrameworkContext, dryRunEnabled: Boolean): Map[String, Long] = {
    // $COVERAGE-OFF$ Disabling scoverage for main and execute method
    val enrolmentDF = loadData(userEnrolmentDBSettings, cassandraFormat, new StructType())
      .withColumn("enrolleddate", UDFUtils.getLatestValue(col("enrolled_date"), col("enrolleddate")))
      .select("userid", "batchid", "courseid", "contentstatus", "enrolleddate", "completedon", "certificates");
    val courseBatchDF = loadData(collectionBatchDBSettings, cassandraFormat, new StructType())
      .withColumn("startdate", UDFUtils.getLatestValue(col("start_date"), col("startdate")))
      .withColumn("enddate", UDFUtils.getLatestValue(col("end_date"), col("enddate")))
      .select("batchid", "name", "startdate", "enddate");
    
    val joinedDF = enrolmentDF.join(courseBatchDF, "batchid").cache();
    
    val missingEnrolmentDate = joinedDF.filter(col("enrolleddate").isNull).count()
    val missingCompletionDate = joinedDF.filter(col("completedon").isNull).count()
    
    val enrolmentDateLTBatchStartDate = joinedDF
      .withColumn("enrolledon", unix_timestamp(col("enrolleddate"), "yyyy-MM-dd HH:mm:ss:SSS"))
      .withColumn("startedon", unix_timestamp(col("startdate"), "yyyy-MM-dd"))
      .filter(col("startedon") > col("enrolledon"))
      .count()
    
    val completionDateGTBatchEndDate = joinedDF
      .withColumn("endedon", unix_timestamp(date_add(to_date(col("enddate"), "yyyy-MM-dd"), 1)))
      .withColumn("completedts", unix_timestamp(col("completedon")))
      .filter(col("completedts") > col("endedon"))
      .count()
    
    val notcompletedEnrolments = joinedDF.filter(col("completedon").isNull).cache();
    val distinctCourses = notcompletedEnrolments.select("courseid").distinct()
    val coursesDF = getCollectionLeafNodes();
    val joinedCoursesDF = distinctCourses.join(coursesDF, "courseid")
    
    val progressCompleteDF = notcompletedEnrolments.join(joinedCoursesDF, "courseid").withColumn("contentstatus", updateContentStatusMap(col("contentstatus"), col("leafnodes"))).withColumn("completed", completed(col("contentstatus"), col("leafnodescount"))).cache();
    val missingCompletionDateCount = progressCompleteDF.filter(col("completed") === "Yes").count();

    notcompletedEnrolments.unpersist(true);
    joinedDF.unpersist(true);
    Map("missingEnrolmentDate" -> missingEnrolmentDate, "missingCompletionDate" -> missingCompletionDate, "batchStartDtGTEnrolmentDt" -> enrolmentDateLTBatchStartDate, "completionDtGTBatchEndDt" -> completionDateGTBatchEndDate, "missingCompletionDateCount" -> missingCompletionDateCount)
  }

  def reconcileProgressUpdates()(implicit spark: SparkSession, fc: FrameworkContext, dryRunEnabled: Boolean) : Map[String, Long] = {
    val enrolmentDF = loadData(userEnrolmentDBSettings, cassandraFormat, new StructType());
    val notcompletedEnrolments = enrolmentDF.filter(col("completedon").isNull).cache();
    val distinctCourses = notcompletedEnrolments.select("courseid").distinct()
    val coursesDF = getCollectionLeafNodes();
    val joinedCoursesDF = distinctCourses.join(coursesDF, "courseid")

    val nonCompleteEnrolmentsDF = notcompletedEnrolments.select("userid", "batchid", "courseid", "contentstatus", "enrolleddate")
    val enrolmentCourseJoinedDF = nonCompleteEnrolmentsDF.join(joinedCoursesDF, "courseid")
      .withColumn("contentstatusupdated", contentStatusUpdated(col("contentstatus"), col("leafnodes")))
      .withColumn("contentstatus", updateContentStatusMap(col("contentstatus"), col("leafnodes")))
      .withColumn("completed", completed(col("contentstatus"), col("leafnodescount"))).cache();

    val contentStatusMapUpdCount = enrolmentCourseJoinedDF.filter(col("contentstatusupdated") === "Yes").count();
    val missingCompletionDateCount = enrolmentCourseJoinedDF.filter(col("completed") === "Yes").count();

    if (contentStatusMapUpdCount > 0) {
      logTime(updateContentStatus(enrolmentCourseJoinedDF), "Time taken to update content status map for incomplete enrolments")
    }

    if (missingCompletionDateCount > 0) {
      logTime(updateCompletions(enrolmentCourseJoinedDF), "Time taken to update completion dates")
    }
    notcompletedEnrolments.unpersist(true);
    enrolmentCourseJoinedDF.unpersist(true);
    Map("contentStatusMapUpdCount" -> contentStatusMapUpdCount, "missingCompletionDateCount" -> missingCompletionDateCount)
  }

  def reconcileMissingCertsAndEnrolmentDates(modelParams: Map[String, AnyRef])(implicit spark: SparkSession, fc: FrameworkContext, dryRunEnabled: Boolean) : Map[String, Long] = {

    // $COVERAGE-OFF$ Disabling scoverage for main and execute method
    implicit val sc = spark.sparkContext;
    val enrolmentDF = loadData(userEnrolmentDBSettings, cassandraFormat, new StructType())
      .withColumn("enrolleddate", UDFUtils.getLatestValue(col("enrolled_date"), col("enrolleddate"))).cache();
    val courseBatchDF = loadData(collectionBatchDBSettings, cassandraFormat, new StructType());
    val courseBatchMinDF = courseBatchDF.withColumn("hasSVGCertificate", hasSVGCertificate(col("cert_templates")))
      .withColumn("startdate", UDFUtils.getLatestValue(col("start_date"), col("startdate")))
      .withColumn("enddate", UDFUtils.getLatestValue(col("end_date"), col("enddate")))
      .select("batchid", "hasSVGCertificate", "startdate", "enddate").cache();
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
    Map("blankEnrolmentCount" -> blankEnrolmentCount, "certIssuedCount" -> certIssueRDD.count());
    
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
    // $COVERAGE-OFF$ Disabling scoverage for main and execute method
    import spark.implicits._
    val courseBatchDF = loadData(collectionBatchDBSettings, cassandraFormat, new StructType())
      .withColumn("enddate", UDFUtils.getLatestValue(col("end_date"), col("enddate")))
      .select("batchid", "enddate");
    val enrolmentDF = enrolmentCourseJoinedDF.filter(col("completed") === "Yes").select("userid", "courseid", "batchid");
    val enrolmentRDD = enrolmentDF.rdd.repartition(30).map(f => UserEnrolment(f.getString(0), f.getString(1), f.getString(2)))

    val joinedRdd = enrolmentRDD.joinWithCassandraTable("sunbird_courses", "user_content_consumption", SomeColumns("contentid", "status", "lastcompletedtime", "last_completed_time"), SomeColumns("userid", "batchid", "courseid"))
    val finalRDD = joinedRdd.filter(f => f._2.getIntOption(1).getOrElse(0) == 2).map(f => (f._1.userid, f._1.batchid, f._1.courseid, f._2.getStringOption(2).orNull, f._2.getStringOption(3).orNull))
    val enrolmentJoinedDF = finalRDD.toDF().withColumnRenamed("_1", "userid").withColumnRenamed("_2", "batchid").withColumnRenamed("_3", "courseid")
      .withColumnRenamed("_4", "lastcompletedtime").withColumnRenamed("_5", "last_completed_time")
      .withColumn("lastcompletedtime", UDFUtils.getLatestValue(col("last_completed_time"), col("lastcompletedtime")))
      .select("userid", "batchid", "courseid", "lastcompletedtime")
    val enrolmentWithRevisedDtDF = enrolmentJoinedDF.groupBy("userid", "batchid", "courseid").agg(max(col("lastcompletedtime")).alias("lastreviseddate")).withColumn("lastrevisedon", unix_timestamp(col("lastreviseddate"), "yyyy-MM-dd HH:mm:ss:SSS"))
    enrolmentWithRevisedDtDF.join(courseBatchDF, "batchid")
      .withColumn("endedon", unix_timestamp(col("enddate"), "yyyy-MM-dd"))
      .withColumn("completedts", when(col("endedon").isNotNull, when(col("endedon") < col("lastrevisedon"), col("endedon")).otherwise(col("lastrevisedon"))).otherwise(col("lastrevisedon")))
      .withColumn("completedon", col("completedts") * 1000)
      .withColumn("status", lit(2))
      .select("userid", "courseid", "batchid", "status", "completedon")
      .write.format(cassandraFormat).options(if(dryRunEnabled) userEnrolmentTempDBSettings else userEnrolmentDBSettings).mode("APPEND").save()
  }
  
  def updateEnrolmentDates(blankEnrolmentDatesDF: DataFrame, courseBatchMinDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, dryRunEnabled: Boolean) = {

    import spark.implicits._
    val enrolmentDF = blankEnrolmentDatesDF.select("userid", "courseid", "batchid");
    val enrolmentRDD = enrolmentDF.rdd.repartition(30).map(f => UserEnrolment(f.getString(0), f.getString(1), f.getString(2)))
    val joinedRdd = enrolmentRDD.joinWithCassandraTable("sunbird_courses", "user_content_consumption", SomeColumns("contentid", "status", "lastcompletedtime", "last_completed_time"), SomeColumns("userid", "batchid", "courseid")).cache();
    val finalRDD = joinedRdd.map(f => (f._1.userid, f._1.batchid, f._1.courseid, f._2.getStringOption(2).getOrElse(null), f._2.getStringOption(3).orNull)).filter(f => f._4 != null || f._5 != null)
    val enrolmentJoinedDF = finalRDD.toDF().withColumnRenamed("_1", "userid").withColumnRenamed("_2", "batchid").withColumnRenamed("_3", "courseid").withColumnRenamed("_4", "lastcompletedtime").withColumnRenamed("_5", "last_completed_time")
      .withColumn("lastcompletedtime", UDFUtils.getLatestValue(col("last_completed_time"), col("lastcompletedtime")))
    val enrolmentWithRevisedDtDF = enrolmentJoinedDF.groupBy("userid", "batchid", "courseid").agg(min(col("lastcompletedtime")).alias("lastreviseddate")).withColumn("lastrevisedon", unix_timestamp(col("lastreviseddate"), "yyyy-MM-dd HH:mm:ss:SSS"))
    
    val finalEnrolmentDF = enrolmentDF.join(enrolmentWithRevisedDtDF, Seq("userid", "batchid", "courseid"), "left_outer")
    finalEnrolmentDF.join(courseBatchMinDF, "batchid")
      .withColumn("startedon", unix_timestamp(col("startdate"), "yyyy-MM-dd"))
      .withColumn("startedtimestamp", when(col("lastrevisedon").isNotNull, when(col("startedon") > col("lastrevisedon"), col("startedon")).otherwise(col("lastrevisedon"))).otherwise(col("startedon")))
      .withColumn("enrolleddate", from_unixtime(col("startedtimestamp"), "yyyy-MM-dd HH:mm:ss:SSSZ"))
      .select("userid", "courseid", "batchid", "enrolleddate")
      .write.format(cassandraFormat).options(if(dryRunEnabled) userEnrolmentTempDBSettings else userEnrolmentDBSettings).mode("APPEND").save()

    blankEnrolmentDatesDF
      .select("userid", "courseid", "batchid", "active")
      .withColumn("active", when(col("active").isNull, true).otherwise(col("active")))
      .write.format(cassandraFormat).options(if (dryRunEnabled) userEnrolmentTempDBSettings else userEnrolmentDBSettings).mode("APPEND").save()
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
      if (statusMap != null && statusMap.size > 0) {
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