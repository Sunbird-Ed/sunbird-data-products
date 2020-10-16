package org.sunbird.analytics.exhaust.collection

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JSONUtils

case class UserAggData(user_id: String, activity_id: String, completedCount: Int, context_id: String)
case class CourseData(courseid: String, leafNodesCount: String, level1Data: List[Level1Data])
case class Level1Data(l1identifier: String, l1leafNodesCount: String)
case class AssessmentData(courseid: String, assessmentIds: List[String])

object ProgressExhaustJob extends optional.Application with BaseCollectionExhaustJob {

  override def getClassName = "org.sunbird.analytics.exhaust.collection.ProgressExhaustJob"
  override def jobName() = "ProgressExhaustJob";
  override def jobId() = "progress-exhaust";
  override def getReportPath() = "progress-exhaust/";
  override def getReportKey() = "progress";

  override def getUserCacheColumns(): Seq[String] = {
    Seq("userid", "state", "district", "orgname", "schooludisecode", "schoolname", "block", "board", "rootorgid")
  }

  private val activityAggDBSettings = Map("table" -> "user_activity_agg", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  private val assessmentAggDBSettings = Map("table" -> "assessment_aggregator", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  private val contentHierarchyDBSettings = Map("table" -> "content_hierarchy", "keyspace" -> AppConf.getConfig("sunbird.content.hierarchy.keyspace"), "cluster" -> "ContentCluster");

  private val filterColumns = Seq("courseid", "collectionName", "batchid", "batchName", "userid",  "state", "district", "orgname", "schooludisecode", "schoolname", "board", "block", "enrolleddate", "completedon", "certificatestatus");
  private val columnsOrder = List("Collection Id", "Collection Name", "Batch Id", "Batch Name", "User UUID", "State", "District", "Org Name", "School Id", "School Name", "Block Name", "Declared Board", "Enrolment Date", "Completion Date",
    "Certificate Status", "Progress", "Total Score")
  private val columnMapping = Map("courseid" -> "Collection Id", "collectionName" -> "Collection Name", "batchid" -> "Batch Id", "batchName" -> "Batch Name", "userid" -> "User UUID",
    "state" -> "State", "district" -> "District", "orgname" -> "Org Name", "schooludisecode" -> "School Id", "schoolname" -> "School Name", "block" -> "Block Name", "board" -> "Declared Board", "enrolleddate" -> "Enrolment Date", "completedon" -> "Completion Date", "completionPercentage" -> "Progress",
    "total_sum_score" -> "Total Score", "certificatestatus" -> "Certificate Status")

  override def processBatch(userEnrolmentDF: DataFrame, collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    val hierarchyData = loadCollectionHierarchy(collectionBatch.collectionId)
    val collectionAggDF = getCollectionAgg(collectionBatch, hierarchyData).withColumn("batchid", lit(collectionBatch.batchId));
    val enrolledUsersToBatch = updateCertificateStatus(userEnrolmentDF).select(filterColumns.head, filterColumns.tail: _*)
    val assessmentAggDF = getAssessmentDF(collectionBatch, hierarchyData);
    val progressDF = getProgressDF(enrolledUsersToBatch, collectionAggDF, assessmentAggDF);
    organizeDF(progressDF, columnMapping, columnsOrder);
  }

  def getProgressDF(userEnrolmentDF: DataFrame, collectionAggDF: DataFrame, assessmentAggDF: DataFrame): DataFrame = {

    val collectionAggPivotDF = collectionAggDF.groupBy("courseid", "batchid", "userid", "completionPercentage").pivot(concat(col("l1identifier"), lit(" - Progress"))).agg(first(col("l1completionPercentage")))
      .drop("null")
    val assessmentAggPivotDF = assessmentAggDF.groupBy("courseid", "batchid", "userid", "total_sum_score")
      .pivot(concat(col("content_id"), lit(" - Score"))).agg(concat(ceil((split(first("grand_total"), "\\/")
      .getItem(0) * 100) / (split(first("grand_total"), "\\/")
      .getItem(1))), lit("%")))
    val progressDF = collectionAggPivotDF.join(assessmentAggPivotDF, Seq("courseid", "batchid", "userid"), "left_outer")
    userEnrolmentDF.join(progressDF, Seq("courseid", "batchid", "userid"), "left_outer")
      .withColumn("completedon", when(col("completedon").isNotNull, date_format(col("completedon"), "dd/MM/yyyy")).when(col("completionPercentage") === 100, date_format(current_date(), "dd/MM/yyyy")).otherwise(""))
      .withColumn("enrolleddate", date_format(to_date(col("enrolleddate")), "dd/MM/yyyy"))
  }

  def updateCertificateStatus(userEnrolmentDF: DataFrame): DataFrame = {
    userEnrolmentDF.withColumn("certificatestatus", when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0, "Issued")
      .when(col("issued_certificates").isNotNull && size(col("issued_certificates").cast("array<map<string, string>>")) > 0, "Issued").otherwise(""))
      .withColumn("board", UDFUtils.extractFromArrayString(col("board")))
  }
  def getAssessmentDF(batch: CollectionBatch, hierarchyData: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

    import spark.implicits._
    val contentDataDF = hierarchyData.rdd.map(row => {
      val hierarchy = JSONUtils.deserialize[Map[String, AnyRef]](row.getString(1))
      // TODO: assessmentTypes - make it configurable.
      filterAssessmentsFromHierarchy(List(hierarchy), List(AppConf.getConfig("assessment.metrics.supported.contenttype")), AssessmentData(row.getString(0), List()))
    }).toDF()
        .select(col("courseid"), explode_outer(col("assessmentIds")).as("contentid"))

    val assessAggdf = loadData(assessmentAggDBSettings, cassandraFormat, new StructType()).where(col("course_id") === batch.collectionId && col("batch_id") === batch.batchId).select("course_id", "batch_id", "user_id", "content_id", "total_max_score", "total_score", "grand_total")
      .withColumnRenamed("user_id", "userid")
      .withColumnRenamed("batch_id", "batchid")
      .withColumnRenamed("course_id", "courseid")

    val dataDF = contentDataDF.join(assessAggdf, contentDataDF.col("courseid") === assessAggdf.col("courseid") && contentDataDF.col("contentid") === assessAggdf.col("content_id"), "inner").select(assessAggdf.col("*"))

    val assessmentAggSpec = Window.partitionBy("userid", "batchid", "courseid")
    dataDF.withColumn("agg_score", sum("total_score") over assessmentAggSpec)
      .withColumn("agg_max_score", sum("total_max_score") over assessmentAggSpec)
      .withColumn("total_sum_score", concat(ceil((col("agg_score") * 100) / col("agg_max_score")), lit("%")))
  }

  def loadCollectionHierarchy(identifier: String)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    loadData(contentHierarchyDBSettings, cassandraFormat, new StructType()).where(col("identifier") === s"${identifier}").select("identifier", "hierarchy")
  }

  def getCollectionAgg(batch: CollectionBatch, hierarchyData: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

    import spark.implicits._
    val userAgg = loadData(activityAggDBSettings, cassandraFormat, new StructType()).where(col("context_id") === s"cb:${batch.batchId}").select("user_id", "activity_id", "agg", "context_id")
      .map(row => {
        UserAggData(row.getString(0), row.getString(1), row.get(2).asInstanceOf[Map[String, Int]]("completedCount"), row.getString(3))
      }).toDF()
    val hierarchyDataDf = hierarchyData.rdd.map(row => {
      val hierarchy = JSONUtils.deserialize[Map[String, AnyRef]](row.getString(1))
      parseCourseHierarchy(List(hierarchy), 0, CourseData(row.getString(0), "0", List()), depthLevel = 2)
    }).toDF()
    val hierarchyDf = hierarchyDataDf.select($"courseid", $"leafNodesCount", $"level1Data", explode_outer($"level1Data").as("exploded_level1Data")).select("courseid", "leafNodesCount", "exploded_level1Data.*")

    val dataDf = hierarchyDf.join(userAgg, hierarchyDf.col("courseid") === userAgg.col("activity_id"), "left")
      .withColumn("completionPercentage", when(userAgg.col("completedCount") >= hierarchyDf.col("leafNodesCount"), 100).otherwise(userAgg.col("completedCount") / hierarchyDf.col("leafNodesCount") * 100).cast("int"))
      .select(userAgg.col("user_id").as("userid"), userAgg.col("context_id").as("contextid"),
        hierarchyDf.col("courseid"), col("completionPercentage"), hierarchyDf.col("l1identifier"), hierarchyDf.col("l1leafNodesCount"))

    val resDf = dataDf.join(userAgg, dataDf.col("l1identifier") === userAgg.col("activity_id") &&
      userAgg.col("context_id") === dataDf.col("contextid") && userAgg.col("user_id") === dataDf.col("userid"), "left")
      .withColumn("batchid", lit(batch.batchId))
      .withColumn("l1completionPercentage", when(userAgg.col("completedCount") >= dataDf.col("l1leafNodesCount"), 100).otherwise(userAgg.col("completedCount") / dataDf.col("l1leafNodesCount") * 100).cast("int"))
      .select("userid", "courseid", "batchid", "completionPercentage", "l1identifier", "l1completionPercentage")

    resDf
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

  def parseCourseHierarchy(data: List[Map[String, AnyRef]], levelCount: Int, prevData: CourseData, depthLevel: Int): CourseData = {
    if (levelCount < depthLevel) {
      val list = data.map(childNodes => {
        val mimeType = childNodes.getOrElse("mimeType", "").asInstanceOf[String]
        val visibility = childNodes.getOrElse("visibility", "").asInstanceOf[String]
        // TODO: need to change to primaryCategory after 3.3.0
        val contentType = childNodes.getOrElse("contentType", "").asInstanceOf[String]
        if ((StringUtils.equalsIgnoreCase(mimeType, "application/vnd.ekstep.content-collection") && StringUtils.equalsIgnoreCase(visibility, "Default") && StringUtils.equalsIgnoreCase(contentType, "Course"))) {
          val identifier = childNodes.getOrElse("identifier", "").asInstanceOf[String]
          val leafNodesCount = childNodes.getOrElse("leafNodesCount", 0).asInstanceOf[Int]
          val courseData = if (levelCount == 0) {
            CourseData(prevData.courseid, leafNodesCount.toString, List())
          } else {
            val prevL1List = prevData.level1Data
            CourseData(prevData.courseid, prevData.leafNodesCount, (prevL1List ::: List(Level1Data(identifier, leafNodesCount.toString))))
          }
          val children = childNodes.getOrElse("children", List()).asInstanceOf[List[Map[String, AnyRef]]]
          if (null != children && children.nonEmpty) {
            parseCourseHierarchy(children, levelCount + 1, courseData, 2)
          } else courseData
        } else prevData
      })
      val courseId = list.head.courseid
      val leafNodeCount = list.head.leafNodesCount
      val level1Data = list.map(x => x.level1Data).flatten.toList
      CourseData(courseId, leafNodeCount, level1Data)
    } else prevData
  }

}