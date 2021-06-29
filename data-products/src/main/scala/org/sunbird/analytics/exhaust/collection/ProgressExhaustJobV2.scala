package org.sunbird.analytics.exhaust.collection

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}

object ProgressExhaustJobV2 extends optional.Application with BaseCollectionExhaustJob {

  override def getClassName = "org.sunbird.analytics.exhaust.collection.ProgressExhaustJobV2"

  override def jobName() = "ProgressExhaustJobV2";

  override def jobId() = "progress-exhaust";

  override def getReportPath() = "progress-exhaust/";

  override def getReportKey() = "progress";
  private val persistedDF: scala.collection.mutable.ListBuffer[DataFrame] = scala.collection.mutable.ListBuffer[DataFrame]();

  override def getUserCacheColumns(): Seq[String] = {
    Seq("userid", "state", "district", "cluster", "orgname", "schooludisecode", "schoolname", "block", "board", "rootorgid", "usertype", "usersubtype")
  }

  override def getEnrolmentColumns(): Seq[String] = {
    Seq("batchid", "userid", "courseid", "active", "certificates", "issued_certificates", "enrolleddate", "completedon", "contentstatus")
  }

  override def unpersistDFs() {
    persistedDF.foreach(f => f.unpersist(true))
  }

  private val activityAggDBSettings = Map("table" -> "user_activity_agg", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  private val contentHierarchyDBSettings = Map("table" -> "content_hierarchy", "keyspace" -> AppConf.getConfig("sunbird.content.hierarchy.keyspace"), "cluster" -> "ContentCluster");

  private val filterColumns = Seq("courseid", "collectionName", "batchid", "batchName", "userid", "state", "district", "orgname", "schooludisecode", "schoolname", "board", "block", "cluster", "usertype", "usersubtype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage");
  private val columnsOrder = List("Collection Id", "Collection Name", "Batch Id", "Batch Name", "User UUID", "State", "District", "Org Name", "School Id",
    "School Name", "Block Name", "Cluster Name", "User Type", "User Sub Type", "Declared Board", "Enrolment Date", "Completion Date", "Certificate Status", "Progress", "Total Score")
  private val columnMapping = Map("courseid" -> "Collection Id", "collectionName" -> "Collection Name", "batchid" -> "Batch Id", "batchName" -> "Batch Name", "userid" -> "User UUID",
    "state" -> "State", "district" -> "District", "orgname" -> "Org Name", "schooludisecode" -> "School Id", "schoolname" -> "School Name", "block" -> "Block Name",
    "cluster" -> "Cluster Name", "usertype" -> "User Type", "usersubtype" -> "User Sub Type", "board" -> "Declared Board", "enrolleddate" -> "Enrolment Date", "completedon" -> "Completion Date",
    "completionPercentage" -> "Progress", "total_sum_score" -> "Total Score", "certificatestatus" -> "Certificate Status")

  override def processBatch(userEnrolmentDF: DataFrame, collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    val hierarchyData = loadCollectionHierarchy(collectionBatch.collectionId)
    val leafNodesCount = getLeafNodeCount(hierarchyData);
    val enrolmentWithCompletions = userEnrolmentDF.withColumn("completionPercentage", UDFUtils.completionPercentage(col("contentstatus"), lit(leafNodesCount)));
    val enrolledUsersToBatch = updateCertificateStatus(enrolmentWithCompletions).select(filterColumns.head, filterColumns.tail: _*)
    // Get selfAssess contents for generate score percentage
    val supportedContentIds: List[AssessmentData] = getContents(hierarchyData, AppConf.getConfig("assessment.metrics.supported.contenttype"))
    val activityAggData: DataFrame = if (supportedContentIds.nonEmpty) {
      getActivityAggData(collectionBatch)
        // filter the selfAssess Contents from the "agg" column
        .withColumn("filteredContents", UDFUtils.filterSupportedContentTypes(col("agg"), typedLit(supportedContentIds.headOption.getOrElse(AssessmentData("", List())).assessmentIds)))
        // Compute the percentage
        .withColumn("scorePercentage", UDFUtils.computePercentage(col("filteredContents")))
        .drop("agg", "filteredContents")
    } else null
    val progressDF = getProgressDF(enrolledUsersToBatch, activityAggData)
    organizeDF(progressDF, columnMapping, columnsOrder)
  }


  def getProgressDF(userEnrolmentDF: DataFrame, aggregateDF: DataFrame): DataFrame = {
    val updatedEnrolmentDF = userEnrolmentDF.withColumn("completionPercentage", when(col("completedon").isNotNull, 100).otherwise(col("completionPercentage")))
      .withColumn("completedon", when(col("completedon").isNotNull, date_format(col("completedon"), "dd/MM/yyyy")).otherwise(""))
      .withColumn("enrolleddate", date_format(to_date(col("enrolleddate")), "dd/MM/yyyy"))
    if (null != aggregateDF) {
      val keys = aggregateDF.select(explode(map_keys(col("scorePercentage")))).distinct().collect().map(f => f.get(0))
      val updatedAggregateKeys = keys.map(f => col("scorePercentage").getItem(f).as(f.toString))
      val updatedAggDF = aggregateDF.select(col("*") +: updatedAggregateKeys: _*)
      updatedEnrolmentDF.join(updatedAggDF, updatedEnrolmentDF.col("userid") === updatedAggDF.col("user_id") && updatedEnrolmentDF.col("courseid") === updatedAggDF.col("activity_id"),
        "left_outer").drop("user_id", "activity_id", "context_id", "scorePercentage")
    } else updatedEnrolmentDF
  }

  def updateCertificateStatus(userEnrolmentDF: DataFrame): DataFrame = {
    userEnrolmentDF.withColumn("certificatestatus", when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0, "Issued")
      .when(col("issued_certificates").isNotNull && size(col("issued_certificates").cast("array<map<string, string>>")) > 0, "Issued").otherwise(""))
      .withColumn("board", UDFUtils.extractFromArrayString(col("board")))
  }

  def getActivityAggData(collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    loadData(activityAggDBSettings, cassandraFormat, new StructType())
      .where(col("context_id") === s"cb:${collectionBatch.batchId}" && col("activity_id") === collectionBatch.collectionId)
      .select("user_id", "activity_id", "agg", "context_id")

  }


  def getContents(hierarchyData: DataFrame, contentType: String)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[AssessmentData] = {
    hierarchyData.rdd.map(row => {
      val hierarchy = JSONUtils.deserialize[Map[String, AnyRef]](row.getString(1))
      filterAssessmentsFromHierarchy(List(hierarchy), List(contentType), AssessmentData(row.getString(0), List()))
    }).collect().toList
  }

  def loadCollectionHierarchy(identifier: String)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    loadData(contentHierarchyDBSettings, cassandraFormat, new StructType()).where(col("identifier") === s"${identifier}").select("identifier", "hierarchy")
  }

  def getLeafNodeCount(hierarchyData: DataFrame): Int = {
    hierarchyData.rdd.map(row => {
      val hierarchy = JSONUtils.deserialize[Map[String, AnyRef]](row.getString(1))
      hierarchy.getOrElse("leafNodesCount", 0).asInstanceOf[Int]
    }).collect().head
  }

  def filterAssessmentsFromHierarchy(data: List[Map[String, AnyRef]], assessmentTypes: List[String], prevData: AssessmentData): AssessmentData = {
    if (data.nonEmpty) {
      val list = data.map(childNode => {
        // TODO: need to change to primaryCategory after 3.3.0
        val contentType = childNode.getOrElse("contentType", "").asInstanceOf[String]
        val updatedIds = (if (assessmentTypes.contains(contentType)) {
          List(childNode.get("identifier").get.asInstanceOf[String])
        } else List()) ::: prevData.assessmentIds
        val updatedAssessmentData = AssessmentData(prevData.courseid, updatedIds)
        val children = childNode.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
        if (null != children && children.nonEmpty) {
          filterAssessmentsFromHierarchy(children, assessmentTypes, updatedAssessmentData)
        } else updatedAssessmentData
      })
      val courseId = list.head.courseid
      val assessmentIds = list.map(x => x.assessmentIds).flatten.distinct
      AssessmentData(courseId, assessmentIds)
    } else prevData
  }
}