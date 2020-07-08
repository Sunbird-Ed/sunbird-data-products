package org.sunbird.analytics.job.report

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.sunbird.analytics.util.ESUtil
import org.sunbird.cloud.storage.conf.AppConf

case class DruidOutput(identifier: String, channel: String)

object AssessmentMetricsJob extends optional.Application with IJob with BaseReportsJob {

  implicit val className = "org.ekstep.analytics.job.AssessmentMetricsJob"

  private val indexName: String = AppConf.getConfig("assessment.metrics.es.index.prefix") + DateTimeFormat.forPattern("dd-MM-yyyy-HH-mm").print(DateTime.now())
  val metrics = scala.collection.mutable.Map[String, BigInt]();
  val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")

  def name(): String = "AssessmentMetricsJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {


    JobLogger.init("Assessment Metrics")
    JobLogger.start("Assessment Job Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig);
    implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig);
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext();
    execute(jobConfig)
  }

  def recordTime[R](block: => R, msg: String): (R) = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    JobLogger.log(msg + (t1 - t0), None, INFO)
    result;
  }


  private def execute(config: JobConfig)(implicit sc: SparkContext, fc: FrameworkContext) = {
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val readConsistencyLevel: String = AppConf.getConfig("assessment.metrics.cassandra.input.consistency")
    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
      .set("spark.sql.caseSensitive", AppConf.getConfig(key = "spark.sql.caseSensitive"))
    implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val druidConfig = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.modelParams.get("druidConfig")))
    val time = CommonUtil.time({
      val reportDF = recordTime(prepareReport(spark, loadData, druidConfig).cache(), s"Time take generate the dataframe} - ")
      val denormalizedDF = recordTime(denormAssessment(reportDF), s"Time take to denorm the assessment - ")
      recordTime(saveReport(denormalizedDF, tempDir), s"Time take to save the all the reports into both azure and es -")
      reportDF.unpersist(true)
    });
    metrics.put("totalExecutionTime", time._1);
    JobLogger.end("AssessmentReport Generation Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "metrics" -> metrics)))
    spark.stop()
    fc.closeContext()
  }

  /**
   * Method used to load the cassnadra table data by passing configurations
   *
   * @param spark    - Spark Sessions
   * @param settings - Cassnadra configs
   * @return
   */
  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame = {
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(settings)
      .load()
  }

  /**
   * Loading the specific tables from the cassandra db.
   */
  def prepareReport(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame, druidQuery: DruidQueryModel)(implicit fc: FrameworkContext): DataFrame = {
    val sunbirdCoursesKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
    val courseBatchDF = loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace)).select("courseid", "batchid", "enddate", "startdate")
    val userCoursesDF = loadData(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .filter(lower(col("active")).equalTo("true"))
      .select(col("batchid"), col("userid"), col("courseid"), col("active")
        , col("completionpercentage"), col("enrolleddate"), col("completedon"))
    val userDF = loadData(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace)).select(col("userid"),
      col("maskedemail"),
      col("firstname"),
      col("lastname"),
      col("maskedphone"),
      col("rootorgid"),
      col("locationids"),
      col("channel")
    ).persist()
    val userOrgDF = loadData(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace)).filter(lower(col("isdeleted")) === "false").select(col("userid"), col("organisationid")).persist()
    val organisationDF = loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .select(col("id"), col("orgname"), col("channel"), col("isrootorg"), col("locationids"), col("orgcode")).persist()
    val locationDF = loadData(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .select(col("id"), col("name"), col("type")).persist()
    val externalIdentityDF = loadData(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .select(col("provider"), col("idtype"), col("externalid"), col("userid")).persist()
    val assessmentProfileDF = loadData(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .select("course_id", "batch_id", "user_id", "content_id", "total_max_score", "total_score", "grand_total")

    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val druidResult = DruidDataFetcher.getDruidData(druidQuery)
    val finalResult = druidResult.map { f => JSONUtils.deserialize[DruidOutput](f) }
    val finalDF = finalResult.toDF()

    /*
   * courseBatchDF has details about the course and batch details for which we have to prepare the report
   * courseBatchDF is the primary source for the report
   * userCourseDF has details about the user details enrolled for a particular course/batch
   * */

    val courseChannelDenormDF = courseBatchDF.join(finalDF, courseBatchDF.col("courseid") === finalDF.col("identifier"), "left_outer")
      .select(courseBatchDF.col("*"), finalDF("channel"))
    val userCourseDenormDF = courseChannelDenormDF.join(userCoursesDF, userCoursesDF.col("batchid") === courseChannelDenormDF.col("batchid"), "inner")
      .select(
        userCoursesDF.col("batchid"),
        col("userid"),
        col("active"),
        courseChannelDenormDF.col("courseid"),
        courseChannelDenormDF.col("channel").as("course_channel"))
    /*
  *userCourseDenormDF lacks some of the user information that need to be part of the report
  *here, it will add some more user details
  * */
    val userDenormDF = userCourseDenormDF
      .join(userDF, Seq("userid"), "inner")
      .select(
        userCourseDenormDF.col("*"),
        col("firstname"),
        col("lastname"),
        col("maskedemail"),
        col("maskedphone"),
        col("rootorgid"),
        col("locationids"),
        col("channel"),
        concat_ws(" ", col("firstname"), col("lastname")).as("username"))

    val custRootOrgId = getCustodianOrgId(spark, loadData)
    val custodianUserDF = generateCustodianOrgUserData(userDenormDF, custRootOrgId, externalIdentityDF, locationDF, organisationDF)
    val stateUserDF = generateStateOrgUserData(custRootOrgId, userDenormDF, organisationDF, locationDF, externalIdentityDF, userOrgDF)

    val userLocationResolvedDF = custodianUserDF.unionByName(stateUserDF)

    /*
  * userDenormDF lacks organisation details, here we are mapping each users to get the organisationids
  * */
    val userRootOrgDF = userDenormDF
      .join(userOrgDF, userOrgDF.col("userid") === userDenormDF.col("userid") && userOrgDF.col("organisationid") === userDenormDF.col("rootorgid"))
      .select(userDenormDF.col("*"), col("organisationid"))

    val userSubOrgDF = userDenormDF
      .join(userOrgDF, userOrgDF.col("userid") === userDenormDF.col("userid") && userOrgDF.col("organisationid") =!= userDenormDF.col("rootorgid"))
      .select(userDenormDF.col("*"), col("organisationid"))

    val rootOnlyOrgDF = userRootOrgDF
      .join(userSubOrgDF, Seq("userid"), "leftanti")
      .select(userRootOrgDF.col("*"))

    val userOrgDenormDF = rootOnlyOrgDF.union(userSubOrgDF)

    val assessmentDF = getAssessmentData(assessmentProfileDF)
    /**
     * Compute the sum of all the worksheet contents score.
     */
    val assessmentAggDf = Window.partitionBy("user_id", "batch_id", "course_id")
    val resDF = assessmentDF
      .withColumn("agg_score", sum("total_score") over assessmentAggDf)
      .withColumn("agg_max_score", sum("total_max_score") over assessmentAggDf)
      .withColumn("total_sum_score", concat(ceil((col("agg_score") * 100) / col("agg_max_score")), lit("%")))
    /**
     * Filter only valid enrolled userid for the specific courseid
     */

    val userAssessmentResolvedDF = userLocationResolvedDF.join(resDF,
      userLocationResolvedDF.col("userid") === resDF.col("user_id")
        && userLocationResolvedDF.col("batchid") === resDF.col("batch_id")
        && userLocationResolvedDF.col("courseid") === resDF.col("course_id"), "inner")
      .select("batchid", "courseid", "userid", "maskedemail", "maskedphone", "username", "district_name",
        "externalid_resolved", "schoolname_resolved", "schoolUDISE_resolved", "state_name",
        "content_id", "total_score", "grand_total", "total_sum_score")

    /*
  * Resolve organisation name from `rootorgid`
  * */
    val resolvedOrgNameDF = userOrgDenormDF
      .join(organisationDF, organisationDF.col("id") === userOrgDenormDF.col("rootorgid"), "left_outer")
      .groupBy("userid" )
      .agg(concat_ws(",", collect_set("orgname")).as("orgname_resolved"))

    /*
  * merge orgName and schoolName based on `userid` and calculate the course progress percentage from `progress` column which is no of content visited/read
  * */

    val reportDF = userAssessmentResolvedDF
      .join(resolvedOrgNameDF, Seq("userid"), "left_outer")

    userDF.unpersist()
    userOrgDF.unpersist()
    externalIdentityDF.unpersist()
    locationDF.unpersist()
    organisationDF.unpersist()

    reportDF
  }

  /**
   * De-norming the assessment report - Adding content name column to the content id
   *
   * @return - Assessment denormalised dataframe
   */
  def denormAssessment(report: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val contentIds: List[String] = recordTime(report.select(col("content_id")).distinct().collect().map(_ (0)).toList.asInstanceOf[List[String]], "Time taken to get the content IDs- ")
    JobLogger.log("ContentIds are" + contentIds, None, INFO)
    val contentMetaDataDF = ESUtil.getAssessmentNames(spark, contentIds, AppConf.getConfig("assessment.metrics.content.index"), AppConf.getConfig("assessment.metrics.supported.contenttype"))
    report.join(contentMetaDataDF, report.col("content_id") === contentMetaDataDF.col("identifier"), "right_outer") // Doing right join since to generate report only for the "SelfAssess" content types
      .select(
        col("name").as("content_name"),
        col("total_sum_score"), report.col("userid"), report.col("courseid"), report.col("batchid"),
        col("grand_total"), report.col("maskedemail"), report.col("district_name"), report.col("maskedphone"),
        report.col("orgname_resolved"), report.col("externalid_resolved"), report.col("schoolname_resolved"),
        report.col("username"), col("state_name"), col("schoolUDISE_resolved"))
  }

  /**
   * Get the Either last updated assessment question or Best attempt assessment
   *
   * @param reportDF - Dataframe, Report df.
   * @return DataFrame
   */
  def getAssessmentData(reportDF: DataFrame): DataFrame = {
    val bestScoreReport = AppConf.getConfig("assessment.metrics.bestscore.report").toBoolean
    val columnName: String = if (bestScoreReport) "total_score" else "last_attempted_on"
    val df = Window.partitionBy("user_id", "batch_id", "course_id", "content_id").orderBy(desc(columnName))
    reportDF.withColumn("rownum", row_number.over(df)).where(col("rownum") === 1).drop("rownum")
  }


  /**
   * This method is used to upload the report the azure cloud service and
   * Index report data into core elastic search.
   * Alias name: cbatch-assessment
   * Index name: cbatch-assessment-24-08-1993-09-30 (dd-mm-yyyy-hh-mm)
   */
  def saveReport(reportDF: DataFrame, url: String)(implicit spark: SparkSession, fc: FrameworkContext): Unit = {
    val result = reportDF.groupBy("courseid").agg(collect_list("batchid").as("batchid"))
    val uploadToAzure = AppConf.getConfig("course.upload.reports.enabled")
    if (StringUtils.isNotBlank(uploadToAzure) && StringUtils.equalsIgnoreCase("true", uploadToAzure)) {
      val courseBatchList = result.collect.map(r => Map(result.columns.zip(r.toSeq): _*))
      save(courseBatchList, reportDF, url, spark)
    } else {
      JobLogger.log("Skipping uploading reports into to azure", None, INFO)
    }
  }

  /**
   * Converting rows into  column (Reshaping the dataframe.)
   * This method converts the name column into header row formate
   * Example:
   * Input DF
   * +------------------+-------+--------------------+-------+-----------+
   * |              name| userid|            courseid|batchid|total_score|
   * +------------------+-------+--------------------+-------+-----------+
   * |Playingwithnumbers|user021|do_21231014887798...|   1001|         10|
   * |     Whole Numbers|user021|do_21231014887798...|   1001|          4|
   * +------------------+---------------+-------+--------------------+----
   *
   * Output DF: After re-shaping the data frame.
   * +--------------------+-------+-------+------------------+-------------+
   * |            courseid|batchid| userid|Playingwithnumbers|Whole Numbers|
   * +--------------------+-------+-------+------------------+-------------+
   * |do_21231014887798...|   1001|user021|                10|            4|
   * +--------------------+-------+-------+------------------+-------------+
   * Example:
   */
  def transposeDF(reportDF: DataFrame): DataFrame = {
    // Re-shape the dataFrame (Convert the content name from the row to column)
    reportDF.groupBy("courseid", "batchid", "userid")
      .pivot("content_name").agg(concat(ceil((split(first("grand_total"), "\\/")
      .getItem(0) * 100) / (split(first("grand_total"), "\\/")
      .getItem(1))), lit("%")))
  }

  def saveToAzure(reportDF: DataFrame, url: String, batchId: String, transposedData: DataFrame): String = {
    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val storageConfig = getStorageConfig(AppConf.getConfig("cloud.container.reports"), AppConf.getConfig("assessment.metrics.cloud.objectKey"))
    val azureData = reportDF.select(
      reportDF.col("externalid_resolved").as("External ID"),
      reportDF.col("userid").as("User ID"),
      reportDF.col("username").as("User Name"),
      reportDF.col("maskedemail").as("Email ID"),
      reportDF.col("maskedphone").as("Mobile Number"),
      reportDF.col("orgname_resolved").as("Organisation Name"),
      reportDF.col("state_name").as("State Name"),
      reportDF.col("district_name").as("District Name"),
      reportDF.col("schoolUDISE_resolved").as("School UDISE Code"),
      reportDF.col("schoolname_resolved").as("School Name"),
      transposedData.col("*"), // Since we don't know the content name column so we are using col("*")
      reportDF.col("total_sum_score").as("Total Score"))
      .drop("userid", "courseid", "batchid")
    azureData.saveToBlobStore(storageConfig, "csv", "report-" + batchId, Option(Map("header" -> "true")), None);
    s"${AppConf.getConfig("cloud.container.reports")}/${AppConf.getConfig("assessment.metrics.cloud.objectKey")}/report-$batchId.csv"

  }

  def saveToElastic(index: String, reportDF: DataFrame, transposedData: DataFrame): Unit = {
    val assessmentReportDF = reportDF.select(
      col("userid").as("userId"),
      col("username").as("userName"),
      col("courseid").as("courseId"),
      col("batchid").as("batchId"),
      col("grand_total").as("score"),
      col("maskedemail").as("maskedEmail"),
      col("maskedphone").as("maskedPhone"),
      col("district_name").as("districtName"),
      col("orgname_resolved").as("rootOrgName"),
      col("externalid_resolved").as("externalId"),
      col("schoolname_resolved").as("subOrgName"),
      col("schoolUDISE_resolved").as("schoolUDISECode"),
      col("state_name").as("stateName"),
      col("total_sum_score").as("totalScore"),
      transposedData.col("*"), // Since we don't know the content name column so we are using col("*")
      col("reportUrl").as("reportUrl")
    ).drop("userid", "courseid", "batchid")
    ESUtil.saveToIndex(assessmentReportDF, index)
  }

  def rollOverIndex(index: String, alias: String): Unit = {
    val indexList = ESUtil.getIndexName(alias)
    if (!indexList.contains(index)) ESUtil.rolloverIndex(index, alias)
  }

  def save(courseBatchList: Array[Map[String, Any]], reportDF: DataFrame, url: String, spark: SparkSession)(implicit fc: FrameworkContext): Unit = {
    val aliasName = AppConf.getConfig("assessment.metrics.es.alias")
    val indexToEs = AppConf.getConfig("course.es.index.enabled")
    courseBatchList.foreach(item => {
      val courseId = item.getOrElse("courseid", "").asInstanceOf[String]
      val batchList = item.getOrElse("batchid", "").asInstanceOf[Seq[String]].distinct
      JobLogger.log(s"Course batch mappings- courseId: $courseId and batchIdList is $batchList ", None, INFO)
      batchList.foreach(batchId => {
        if (!courseId.isEmpty && !batchId.isEmpty) {
          val filteredDF = reportDF.filter(col("courseid") === courseId && col("batchid") === batchId)
          val transposedData = transposeDF(filteredDF)
          val reportData = transposedData.join(reportDF, Seq("courseid", "batchid", "userid"), "inner")
            .dropDuplicates("userid", "courseid", "batchid").drop("content_name")
          try {
            val urlBatch: String = recordTime(saveToAzure(reportData, url, batchId, transposedData), s"Time taken to save the $batchId into azure -")
            val resolvedDF = reportData.withColumn("reportUrl", lit(urlBatch))
            if (StringUtils.isNotBlank(indexToEs) && StringUtils.equalsIgnoreCase("true", indexToEs)) {
              recordTime(saveToElastic(this.getIndexName, resolvedDF, transposedData), s"Time taken to save the $batchId into to es -")
              JobLogger.log("Indexing of assessment report data is success: " + this.getIndexName, None, INFO)
            } else {
              JobLogger.log("Skipping Indexing assessment report into ES", None, INFO)
            }
          } catch {
            case e: Exception => JobLogger.log("File upload is failed due to " + e, None, ERROR)
          }
        } else {
          JobLogger.log("Report failed to create since course_id is " + courseId + "and batch_id is " + batchId, None, ERROR)
        }
      })
    })
    rollOverIndex(getIndexName, aliasName)
  }

  def getIndexName: String = {
    this.indexName
  }

  /**
   * externalIdMapDF - Filter out the external id by idType and provider and Mapping userId and externalId
   *
   * For state user
   * USR_EXTERNAL_IDENTITY.provider=User.channel and USR_EXTERNAL_IDENTITY.idType=USER.channel and fetch the USR_EXTERNAL_IDENTITY.externalid
   *
   * For Cust User
   * USR_EXTERNAL_IDENTITY.idType='declared-ext-id' and USR_EXTERNAL_IDENTITY.provider=ORG.channel
   * fetch USR_EXTERNAL_IDENTITY.id and map with USR_EXTERNAL_IDENTITY.userid
   */
  /*
  * Resolve school Information
  * 1. school name from `orgid`
  * 2. school UDISE code from
  *   2.1 org.orgcode if user is a state user
  *   2.2 externalID.id if user is a self signed up user
  * */

  def generateCustodianOrgUserData(userDF: DataFrame, custodianOrgId: String, externalIdentityDF: DataFrame, locationDF: DataFrame, organisationDF: DataFrame): DataFrame = {

    val userExplodedLocationDF = userDF.withColumn("exploded_location", explode_outer(col("locationids")))
      .select("userid", "exploded_location")

    val userStateDF = userExplodedLocationDF
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "state")
      .select(userExplodedLocationDF.col("userid"), col("name").as("state_name"))

    val userDistrictDF = userExplodedLocationDF
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "district")
      .select(userExplodedLocationDF.col("userid"), col("name").as("district_name"))

    /**
     * Join with the userDF to get one record per user with district and block information
     */

    val custodianOrguserLocationDF = userDF.filter(col("rootorgid") === lit(custodianOrgId))
      .join(userStateDF, Seq("userid"), "inner")
      .join(userDistrictDF, Seq("userid"), "left")
      .select(userDF.col("*"),
        col("state_name"),
        col("district_name")).drop(col("locationids"))

    val custodianUserPivotDF = custodianOrguserLocationDF
      .join(externalIdentityDF, externalIdentityDF.col("userid") === custodianOrguserLocationDF.col("userid"), "left")
      .join(organisationDF, externalIdentityDF.col("provider") === organisationDF.col("channel")
        && organisationDF.col("isrootorg").equalTo(true), "left")
      .groupBy(custodianOrguserLocationDF.col("userid"), organisationDF.col("id"))
      .pivot("idtype", Seq("declared-ext-id", "declared-school-name", "declared-school-udise-code"))
      .agg(first(col("externalid")))
      .select(custodianOrguserLocationDF.col("userid"),
        col("declared-ext-id"),
        col("declared-school-name"),
        col("declared-school-udise-code"),
        organisationDF.col("id").as("user_channel"))

    val custodianUserDF = custodianOrguserLocationDF
      .join(custodianUserPivotDF, Seq("userid"), "left")
      .withColumn("externalid_resolved",
        when(custodianOrguserLocationDF.col("course_channel") === custodianUserPivotDF.col("user_channel"), col("declared-ext-id")).otherwise(""))
      .withColumn("schoolname_resolved",
        when(custodianOrguserLocationDF.col("course_channel") === custodianUserPivotDF.col("user_channel"), col("declared-school-name")).otherwise(""))
      .withColumn("schoolUDISE_resolved",
        when(custodianOrguserLocationDF.col("course_channel") === custodianUserPivotDF.col("user_channel"), col("declared-school-udise-code")).otherwise(""))
      .select(custodianOrguserLocationDF.col("*"),
        col("externalid_resolved"),
        col("schoolname_resolved"),
        col("schoolUDISE_resolved"))
    custodianUserDF
  }

  def generateStateOrgUserData(custRootOrgId: String, userDF: DataFrame, organisationDF: DataFrame, locationDF: DataFrame,
                               externalIdentityDF: DataFrame, userOrgDF: DataFrame): DataFrame = {

    val stateOrgExplodedDF = organisationDF.withColumn("exploded_location", explode_outer(col("locationids")))
      .select(col("id"), col("exploded_location"))

    val orgStateDF = stateOrgExplodedDF.join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "state")
      .select(stateOrgExplodedDF.col("id"), col("name").as("state_name"))

    val orgDistrictDF = stateOrgExplodedDF
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "district")
      .select(stateOrgExplodedDF.col("id"), col("name").as("district_name"))

    val stateOrgLocationDF = organisationDF
      .join(orgStateDF, Seq("id"))
      .join(orgDistrictDF, Seq("id"), "left")
      .select(organisationDF.col("id").as("orgid"), col("orgname"),
        col("orgcode"), col("isrootorg"), col("state_name"), col("district_name"))

    val subOrgDF = userOrgDF
      .join(stateOrgLocationDF, userOrgDF.col("organisationid") === stateOrgLocationDF.col("orgid")
        && stateOrgLocationDF.col("isrootorg").equalTo(false))
      .dropDuplicates(Seq("userid"))
      .select(col("userid"), stateOrgLocationDF.col("*"))

    val stateUserLocationResolvedDF = userDF.filter(col("rootorgid") =!= lit(custRootOrgId))
      .join(subOrgDF, Seq("userid"), "left")
      .select(userDF.col("*"),
        subOrgDF.col("orgname").as("declared-school-name"),
        subOrgDF.col("orgcode").as("declared-school-udise-code"),
        subOrgDF.col("state_name"),
        subOrgDF.col("district_name")).drop(col("locationids"))

    val stateUserDF = stateUserLocationResolvedDF.as("state_user")
      .join(externalIdentityDF, externalIdentityDF.col("idtype") === col("state_user.channel")
        && externalIdentityDF.col("provider") === col("state_user.channel")
        && externalIdentityDF.col("userid") === col("state_user.userid"), "left")
      .select(col("state_user.*"), externalIdentityDF.col("externalid"))

    val stateDenormUserDF = stateUserDF
      .withColumn("externalid_resolved",
        when(col("course_channel") === col("rootorgid"), col("externalid")).otherwise(""))
      .withColumn("schoolname_resolved",
        when(col("course_channel") === col("rootorgid"), col("declared-school-name")).otherwise(""))
      .withColumn("schoolUDISE_resolved",
        when(col("course_channel") === col("rootorgid"), col("declared-school-udise-code")).otherwise(""))
      .drop("externalid", "declared-school-name", "declared-school-udise-code")
    stateDenormUserDF
  }


  def getUserSelfDeclaredDetails(userDF: DataFrame, custRootOrgId: String, externalIdentityDF: DataFrame, locationDF: DataFrame): DataFrame = {

    val filterUserIdDF = userDF.filter(col("rootorgid") === lit(custRootOrgId))
      .select("userid", "course_channel", "rootorgid", "locationids")

    val extIdDF = externalIdentityDF
      .join(filterUserIdDF, Seq("userid"), "inner")
      .groupBy("userid", "course_channel", "rootorgid")
      .pivot("idtype", Seq("declared-ext-id", "declared-school-name", "declared-school-udise-code"))
      .agg(first(col("externalid")))
      .na.drop("all", Seq("declared-ext-id", "declared-school-name", "declared-school-udise-code"))

    val stateInfoByUserDF = filterUserIdDF.withColumn("exploded_location", explode(col("locationids")))
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "state")
      .withColumn("statename_resolved",
        when(filterUserIdDF.col("course_channel") === filterUserIdDF.col("rootorgid"), col("name"))
          .otherwise(""))
      .select(col("statename_resolved"), col("userid"))

    val denormUserDF = extIdDF.join(stateInfoByUserDF, Seq("userid"), "left_outer")

    val resolvedUserDetails = denormUserDF
      .withColumn("externalid_resolved",
        when(filterUserIdDF.col("course_channel") === filterUserIdDF.col("rootorgid"), denormUserDF.col("declared-ext-id")).otherwise(""))
      .withColumn("schoolname_resolved",
        when(filterUserIdDF.col("course_channel") === filterUserIdDF.col("rootorgid"), denormUserDF.col("declared-school-name")).otherwise(""))
      .withColumn("schoolUDISE_resolved",
        when(filterUserIdDF.col("course_channel") === filterUserIdDF.col("rootorgid"), denormUserDF.col("declared-school-udise-code")).otherwise(""))
      .select(col("userid"), col("externalid_resolved"), col("schoolname_resolved"), col("schoolUDISE_resolved"), col("statename_resolved"))
    resolvedUserDetails
  }

  def getStateDeclaredDetails(userDenormDF: DataFrame, custRootOrgId: String, externalIdentityDF: DataFrame, organisationDF: DataFrame, userOrgDF: DataFrame, locationDF: DataFrame): DataFrame = {

    val stateExternalIdDF = externalIdentityDF
      .join(userDenormDF,
        externalIdentityDF.col("idtype") === userDenormDF.col("channel")
          && externalIdentityDF.col("provider") === userDenormDF.col("channel")
          && externalIdentityDF.col("userid") === userDenormDF.col("userid"), "inner")
      .select(externalIdentityDF.col("userid"), col("externalid") , col("rootorgid"), col("course_channel"))

    val schoolInfoByState = userOrgDF.join(organisationDF,
      organisationDF.col("id") === userOrgDF.col("organisationid"), "left_outer")
      .select(col("userid"), col("orgname"), col("orgcode"))

    val locationidDF = userDenormDF.join(organisationDF, organisationDF.col("id") === userDenormDF.col("rootorgid")
      && organisationDF.col("isrootorg").equalTo(true))
      .select(organisationDF.col("locationids"), userDenormDF.col("userid"), userDenormDF.col("rootorgid"), userDenormDF.col("course_channel"))

    val stateInfoDF = locationidDF.withColumn("exploded_location", explode(col("locationids")))
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "state")
      .withColumn("statename_resolved",
        when(locationidDF.col("course_channel") === locationidDF.col("rootorgid"), col("name")).otherwise(""))
      .dropDuplicates(Seq("userid"))
      .select(col("statename_resolved"), locationidDF.col("userid"))

    val denormStateDetailDF = schoolInfoByState
      .join(stateExternalIdDF, Seq("userid"), "left_outer")
      .join(stateInfoDF, Seq("userid"), "left_outer")
      .withColumn("externalid_resolved",
        when(stateExternalIdDF.col("course_channel") === stateExternalIdDF.col("rootorgid"), stateExternalIdDF.col("externalid")).otherwise(""))
      .withColumn("schoolname_resolved",
        when(stateExternalIdDF.col("course_channel") === stateExternalIdDF.col("rootorgid"), schoolInfoByState.col("orgname")).otherwise(""))
      .withColumn("schoolUDISE_resolved",
        when(stateExternalIdDF.col("course_channel") === stateExternalIdDF.col("rootorgid"), schoolInfoByState.col("orgcode")).otherwise(""))
      .select(schoolInfoByState.col("userid"),
        col("externalid_resolved"),
        col("schoolname_resolved"),
        col("schoolUDISE_resolved"),
        col("statename_resolved"))
    denormStateDetailDF
  }

  def getCustodianOrgId(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): String = {
    val systemSettingDF = loadData(spark, Map("table" -> "system_settings", "keyspace" -> sunbirdKeyspace))
      .where(col("id") === "custodianOrgId" && col("field") === "custodianOrgId")
      .select(col("value")).persist()

    systemSettingDF.select("value").first().getString(0)
  }
}