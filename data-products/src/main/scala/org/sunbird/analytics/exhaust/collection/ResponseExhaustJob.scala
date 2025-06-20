package org.sunbird.analytics.exhaust.collection

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}

object ResponseExhaustJob extends BaseCollectionExhaustJob {

  override def getClassName = "org.sunbird.analytics.exhaust.collection.ResponseExhaustJob"

  override def jobName() = "ResponseExhaustJob";

  override def jobId() = "response-exhaust";

  override def getReportPath() = "response-exhaust/";

  override def getReportKey() = "response";
  private val persistedDF: scala.collection.mutable.ListBuffer[DataFrame] = scala.collection.mutable.ListBuffer[DataFrame]();


  private val defaultObjectType = "QuestionSet";

  override def getUserCacheColumns(): Seq[String] = {
    Seq("userid", "firstname", "lastname", "email", "orgname", "rootorgid", "usertype", "username", "cin", "fmpsid", "province")
  }

  override def getEnrolmentColumns(): Seq[String] = {
    Seq("batchid", "userid", "courseid", "active", "certificates", "issued_certificates", "enrolleddate", "completedon", "contentstatus")
  }

  override def unpersistDFs() {
    persistedDF.foreach(f => f.unpersist(true))
  }

  private val activityAggDBSettings = Map("table" -> "user_activity_agg", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  private val assessmentAggDBSettings = Map("table" -> "assessment_aggregator", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");
  private val contentHierarchyDBSettings = Map("table" -> "content_hierarchy", "keyspace" -> AppConf.getConfig("sunbird.content.hierarchy.keyspace"), "cluster" -> "ContentCluster");
  private val userContentConsumptionDBSettings = Map("table" -> "user_content_consumption", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster");

  private val filterColumns = Seq("courseid", "coursecode", "collectionName", "batchid", "batchName", "userid", "orgname", "usertype", "enrolleddate", "completedon", "certificatestatus", "completionPercentage", "firstname", "lastname", "username", "email", "cin", "fmpsid", "province", "learnerprofile");

  private val columnsOrder = List("User ID", "First Name", "Last Name", "User Name", "Email ID", "FMPS ID", "CIN", "Province", "Org Name", "User Type", "Learner Profile", "Course Code", "Course Name",
    "Course ID", "Batch Id", "Batch Name", "Enrolment Date", "Completion Date", "Certificate Status")

  private val columnMapping = Map("courseid" -> "Course ID", "collectionName" -> "Course Name", "coursecode" -> "Course Code", "learnerprofile" -> "Learner Profile", "batchid" -> "Batch Id", "batchName" -> "Batch Name", "userid" -> "User ID", "orgname" -> "Org Name", "usertype" -> "User Type", "enrolleddate" -> "Enrolment Date", "completedon" -> "Completion Date", "certificatestatus" -> "Certificate Status", "username" -> "User Name", "firstname" -> "First Name", "lastname" -> "Last Name", "email" -> "Email ID", "fmpsid" -> "FMPS ID", "cin" -> "CIN", "province" -> "Province")

  override def processBatch(userEnrolmentDF: DataFrame, collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    val hierarchyData = loadCollectionHierarchy(collectionBatch.collectionId)

    //val collectionAggDF = getCollectionAggWithModuleData(collectionBatch, hierarchyData).withColumn("batchid", lit(collectionBatch.batchId));
    //val enrolledUsersToBatch = updateCertificateStatus(userEnrolmentDF).select(filterColumns.head, filterColumns.tail: _*)
    val assessmentAggDF = getAssessmentDF(collectionBatch, userEnrolmentDF, hierarchyData);
    val leafNodesCount = getLeafNodeCount(hierarchyData);
    val enrolmentWithCompletions = userEnrolmentDF.withColumn("completionPercentage", UDFUtils.completionPercentage(col("contentstatus"), lit(leafNodesCount)));
    val enrolledUsersToBatch = updateCertificateStatus(enrolmentWithCompletions).select(filterColumns.head, filterColumns.tail: _*)
    //val progressDF = getProgressDF(enrolledUsersToBatch, collectionAggDF, assessmentAggDF);
    val progressDF = getProgressDF(enrolledUsersToBatch, null, assessmentAggDF);
    val userSummaryDF = progressDF.join(getUserContentConsumption(collectionBatch), Seq("courseid", "userid", "batchid"), "inner")
      .drop("completionPercentage")
    //.drop("total_sum_score")
    organizeDF(userSummaryDF, columnMapping, columnsOrder);
  }

  def getUserContentConsumption(collectionBatch: CollectionBatch)
                               (implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

    // 1. Load data from Cassandra and materialize in Spark before using Spark SQL expressions
    val rawDF = loadData(userContentConsumptionDBSettings, cassandraFormat, new StructType())
      .select("userid", "contentid", "status", "courseid", "batchid")
      .filter(col("courseid") === collectionBatch.collectionId && col("batchid") === collectionBatch.batchId)
      .cache() // Materialize in Spark to avoid pushdown errors

    val rawDFWithStatus = rawDF.withColumn("status", when(col("status") === 2, lit("COMPLETED")).otherwise(lit("INCOMPLETE")))

    // Extract unique content IDs from rawDFWithStatus
    val contentIds = rawDFWithStatus.select("contentid").distinct().collect().map(_.getString(0)).toList

    // Call getContentNames and convert result to DataFrame
    val contentNamesList = getContentNames(contentIds)
    import spark.implicits._
    val contentNamesDF = contentNamesList
      .map(row => (row.getOrElse("identifier", "").toString, row.getOrElse("name", "").toString))
      .toDF("identifier", "name")

    // Join rawDFWithStatus with contentNamesDF to get content name for each contentid
    val rawWithNamesDF = rawDFWithStatus.join(contentNamesDF, rawDFWithStatus("contentid") === contentNamesDF("identifier"), "left")
      .withColumnRenamed("name", "contentname")
      .drop("identifier")

    // Pivot on contentname instead of contentid
    val pivotedDF = rawWithNamesDF
      .groupBy("userid", "courseid", "batchid")
      .pivot("contentname")
      .agg(first("status"))

    // Rename pivoted columns to include _status suffix
    val renamedCols = pivotedDF.columns.map { colName =>
      if (!Set("userid", "courseid", "batchid").contains(colName)) {
        when(pivotedDF(colName).isNull, lit("INCOMPLETE"))
          .otherwise(pivotedDF(colName))
          .as(s"${colName} - Status")
      } else {
        pivotedDF(colName)
      }
    }

    pivotedDF.select(renamedCols: _*)
  }



  def getProgressDF(userEnrolmentDF: DataFrame, collectionAggDF: DataFrame, assessmentAggDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

    //    val collectionAggPivotDF = collectionAggDF.groupBy("courseid", "batchid", "userid", "completionPercentage").pivot(concat(col("l1identifier"), lit(" - Progress"))).agg(first(col("l1completionPercentage")))
    //      .drop("null")
    //    val assessmentAggPivotDF = assessmentAggDF.withColumn("content_score", concat(col("content_id"), lit(" - Score")))
    //      .groupBy("courseid", "batchid", "userid", "total_sum_score")
    //      .pivot("content_score").agg(concat(ceil((split(first("grand_total"), "\\/")
    //        .getItem(0) * 100) / (split(first("grand_total"), "\\/")
    //        .getItem(1))), lit("%")))


    val contentIds = assessmentAggDF.select("content_id").distinct().collect().map(_.getString(0)).toList
    val contentNamesList = getContentNames(contentIds)
    import spark.implicits._
    val contentNamesDF = contentNamesList
      .map(row => (row.getOrElse("identifier", "").toString, row.getOrElse("name", "").toString))
      .toDF("identifier", "name")

   val joinedAssessmentDF  = assessmentAggDF.join(contentNamesDF, assessmentAggDF("content_id") === contentNamesDF("identifier"), "left")



    val assessmentAggPivotDF = joinedAssessmentDF
      .withColumn("content_score", concat(col("name"), lit(" - Score")))
      .groupBy("courseid", "batchid", "userid")
      .pivot("content_score")
      .agg(concat(
        split(first("grand_total"), "\\/").getItem(0),
        lit("/"),
        split(first("grand_total"), "\\/").getItem(1)
      ))


    //val progressDF = collectionAggPivotDF.join(assessmentAggPivotDF, Seq("courseid", "batchid", "userid"), "left_outer")
    //userEnrolmentDF.join(progressDF, Seq("courseid", "batchid", "userid"), "left_outer")
    userEnrolmentDF.join(assessmentAggPivotDF, Seq("courseid", "batchid", "userid"), "left_outer")
      .withColumn("completionPercentage", when(col("completedon").isNotNull, 100).otherwise(col("completionPercentage")))
      .withColumn("completedon", when(col("completedon").isNotNull, date_format(col("completedon"), "dd/MM/yyyy")).otherwise(""))
      .withColumn("enrolleddate", date_format(to_date(col("enrolleddate")), "dd/MM/yyyy"))
  }

  def updateCertificateStatus(userEnrolmentDF: DataFrame): DataFrame = {
    userEnrolmentDF.withColumn("certificatestatus", when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0, "Issued")
      .when(col("issued_certificates").isNotNull && size(col("issued_certificates").cast("array<map<string, string>>")) > 0, "Issued").otherwise(""))
    //.withColumn("board", UDFUtils.extractFromArrayString(col("board")))
  }

  def filterAssessmentDF(assessmentDF: DataFrame): DataFrame = {
    val bestScoreReport = AppConf.getConfig("assessment.metrics.bestscore.report").toBoolean
    val columnName: String = if (bestScoreReport) "total_score" else "last_attempted_on"
    val df = Window.partitionBy("userid", "batchid", "courseid", "content_id").orderBy(desc(columnName))
    assessmentDF.withColumn("rownum", row_number.over(df)).where(col("rownum") === 1).drop("rownum")
  }

  def getAssessmentAggData(userEnrolmentDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    val df = loadData(assessmentAggDBSettings, cassandraFormat, new StructType())
      .select("course_id", "batch_id", "user_id", "content_id", "total_max_score", "total_score", "grand_total", "last_attempted_on")
      .withColumnRenamed("user_id", "userid")
      .withColumnRenamed("batch_id", "batchid")
      .withColumnRenamed("course_id", "courseid")
    val assessmentDF = userEnrolmentDF.join(df, Seq("userid", "courseid", "batchid"), "inner").persist()
    persistedDF.append(assessmentDF)
    assessmentDF
  }

  def getAssessmentDF(batch: CollectionBatch, userEnrolmentDF: DataFrame, hierarchyData: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

    import spark.implicits._
    val contentDataDF = hierarchyData.rdd.map(row => {
        val hierarchy = JSONUtils.deserialize[Map[String, AnyRef]](row.getString(1))
        val objectTypeFilter = Option(AppConf.getConfig("assessment.metrics.supported.objecttype")).getOrElse("")
        val questionTypes = if (objectTypeFilter.isEmpty) defaultObjectType else objectTypeFilter

        val assessmentTypes = AppConf.getConfig("assessment.metrics.supported.contenttype").split(",").toList :+ "Resource"
        val primaryCategories = AppConf.getConfig("assessment.metrics.supported.primaryCategories").split(",").toList :+ "Course Assessment" :+ "Practise Assess"
        val assessmentFilters = Map(
          "assessmentTypes" -> assessmentTypes,
          "questionTypes" -> questionTypes.split(",").toList,
          "primaryCategories" -> primaryCategories
        )

        filterAllAssessmentsFromHierarchy(List(hierarchy), assessmentFilters, AssessmentData(row.getString(0), List()))
      }).toDF()
      .select(col("courseid"), explode_outer(col("assessmentIds")).as("contentid"))

    val assessAggdf = filterAssessmentDF(getAssessmentAggData(userEnrolmentDF))
      .select("courseid", "batchid", "userid", "content_id", "total_max_score", "total_score", "grand_total")

    val dataDF = contentDataDF.join(assessAggdf, contentDataDF.col("courseid") === assessAggdf.col("courseid")
      && contentDataDF.col("contentid") === assessAggdf.col("content_id"), "inner").select(assessAggdf.col("*"))
    val assessmentAggSpec = Window.partitionBy("userid", "batchid", "courseid")

    val df = dataDF.withColumn("agg_score", sum("total_score") over assessmentAggSpec)
      .withColumn("agg_max_score", sum("total_max_score") over assessmentAggSpec)
    //.withColumn("total_sum_score", concat(col("agg_score"), lit("/"), col("agg_max_score"))).persist()
    //.withColumn("total_sum_score", concat(ceil((col("agg_score") * 100) / col("agg_max_score")), lit("%"))).persist()
    persistedDF.append(df);
    df;
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

  def getCollectionAggWithModuleData(batch: CollectionBatch, hierarchyData: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

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

    resDf.cache()
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
      val level1Data = list.flatMap(x => x.level1Data)
      CourseData(courseId, leafNodeCount, level1Data)
    } else prevData
  }
}