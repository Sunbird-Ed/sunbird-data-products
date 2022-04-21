package org.sunbird.analytics.exhaust.collection

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
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
  private val defaultObjectType = "QuestionSet";

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
    val supportedContentIds: List[AssessmentData] = getContents(hierarchyData)
    val activityAggData: DataFrame = if (supportedContentIds.nonEmpty) {
      getActivityAggData(collectionBatch)
        // filter the selfAssess Contents from the "agg" column
        .withColumn("filteredContents", filterSupportedContentTypes(col("aggregates"), typedLit(supportedContentIds.headOption.getOrElse(AssessmentData("", List())).assessmentIds)))
        // Compute the percentage for filteredContents Map
        .withColumn("scorePercentage", computePercentage(col("filteredContents")))
        .drop("aggregates", "filteredContents")
    } else null
    val progressDF = getProgressDF(enrolledUsersToBatch, activityAggData)
    organizeDF(progressDF, columnMapping, columnsOrder)
  }


  def getProgressDF(userEnrolmentDF: DataFrame, aggregateDF: DataFrame): DataFrame = {
    val updatedEnrolmentDF = userEnrolmentDF.withColumn("completionPercentage", when(col("completedon").isNotNull, 100).otherwise(col("completionPercentage")))
      .withColumn("completedon", when(col("completedon").isNotNull, date_format(col("completedon"), "dd/MM/yyyy")).otherwise(""))
      .withColumn("enrolleddate", date_format(to_date(col("enrolleddate")), "dd/MM/yyyy"))
    if (null != aggregateDF) {
      // Keys - All the keys of scorePercentage column ex: Keys["total_sum_score","do_1128870328040161281204 - Score"]
      val keys: Array[String] = aggregateDF.select(explode(map_keys(col("scorePercentage")))).distinct().collect().map(f => f.get(0).asInstanceOf[String])
      // Converting the map object to dataframe columns format ex = [do_1128870328040161281204 - Score -> 100%, total_sum_score -> 100.0%] to df column and value
      val updatedAggregateKeys: Array[Column] = keys.map(f => aggregateDF.col("scorePercentage").getItem(f).as(f))
      // Merge the aggregate columns and score metrics columns
      val updatedAggDF:DataFrame = aggregateDF.select(col("*") +: updatedAggregateKeys: _*)
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
    val dataDf = loadData(activityAggDBSettings, cassandraFormat, new StructType())
      .where(col("context_id") === s"cb:${collectionBatch.batchId}")
      .select("user_id", "activity_id", "aggregates", "context_id").persist()
    persistedDF.append(dataDf)
    dataDf.filter(col("activity_id") === collectionBatch.collectionId)
  }


  def getContents(hierarchyData: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): List[AssessmentData] = {
    val objectTypeFilter = Option(AppConf.getConfig("assessment.metrics.supported.objecttype")).getOrElse("")
    val questionTypes = if (objectTypeFilter.isEmpty) defaultObjectType else objectTypeFilter

    val assessmentFilters = Map(
      "assessmentTypes" -> AppConf.getConfig("assessment.metrics.supported.contenttype").split(",").toList,
      "questionTypes" -> questionTypes.split(",").toList,
      "primaryCategories" -> AppConf.getConfig("assessment.metrics.supported.primaryCategories").split(",").toList
    )
    hierarchyData.rdd.map(row => {
      val hierarchy = JSONUtils.deserialize[Map[String, AnyRef]](row.getString(1))
      filterAssessmentsFromHierarchy(List(hierarchy), assessmentFilters, AssessmentData(row.getString(0), List()))
    }).collect().toList
  }

  def loadCollectionHierarchy(identifier: String)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    loadData(contentHierarchyDBSettings, cassandraFormat, new StructType()).where(col("identifier") === s"${identifier}").select("identifier", "hierarchy")
  }

  def getLeafNodeCount(hierarchyData: DataFrame): Int = {
    val hierarchyStr = hierarchyData.first().getString(1)
    val hierarchy = JSONUtils.deserialize[Map[String, AnyRef]](hierarchyStr)
    hierarchy.getOrElse("leafNodesCount", 0).asInstanceOf[Int]
  }

  /**
   * This method is used to compute the score percentage of total contents, individual contents
   *
   * Example  Input: agg = Map(
   * 'completedCount' -> 26,
   * "max_score:do_3131799116748881921729" -> 5,
   * "max_score:do_3131799201424179201659" -> 1,
   * "max_score:do_3131799335459307521660" -> 10,
   * "score:do_3131799116748881921729" -> 3,
   * "score:do_3131799201424179201659" -> 1,
   * "score:do_3131799335459307521660" ->  1)
   *
   * result = Map(
   * "total_sum_score" -> (3+1+1)/(5+1+10),
   * "do_3131799116748881921729 - Score" -> ((3*100)/5)%,
   * "do_3131799201424179201659 - Score" -> ((1*100)/1)%,
   * "do_3131799335459307521660 - Score" -> ((1*100)/10)%
   * )
   * // As per previous report format
   *
   */
  def computePercentageFn(agg: Map[String, Double]): Map[String, String] = {
    val contentScoreList = agg.filter(x => x._1.startsWith("score"))
    val total_max_score = agg.filter(x => x._1.startsWith("max_score")).foldLeft(0D)(_ + _._2)
    if (contentScoreList.nonEmpty && total_max_score > 0) {
      val total_score = contentScoreList.foldLeft(0D)(_ + _._2)
      val total_score_percentage = Math.ceil((total_score / total_max_score) * 100).toInt
      val contentScoreInPercentage: Map[String, String] = contentScoreList.map(x => {
        val contentScore = x._2 * 100
        val contentMaxScore = agg.getOrElse(s"max_score:${x._1.split(":")(1)}", 0D)
        val contentScoreInPercentage = if (contentMaxScore > 0) Math.ceil(contentScore / contentMaxScore).toInt.toString.concat("%") else ""
        Map(s"${x._1.split(":")(1)} - Score" -> contentScoreInPercentage)
      }).flatten.toMap
      contentScoreInPercentage ++ Map("total_sum_score" -> total_score_percentage.toString.concat("%"))
    } else {
      Map("total_sum_score" -> "")
    }
  }
  val computePercentage = udf[Map[String, String], Map[String, Double]](computePercentageFn)

  /**
   *  This UDF function used to filter the only selfAssess supported contents
   */
  def filterSupportedContentTypesFn(agg: Map[String, Double], supportedContents: Seq[String]): Map[String, Double] = {
    val identifiers = agg.filter(x => x._1.startsWith("score")).keys.map(y => y.split(":")(1))
    val assessContentIdentifiers = identifiers.filter(identifier => supportedContents.contains(identifier))
    agg.filter(key => assessContentIdentifiers.exists(x => key._1.contains(x)))
  }

  val filterSupportedContentTypes = udf[Map[String, Double], Map[String, Double], Seq[String]](filterSupportedContentTypesFn)
}