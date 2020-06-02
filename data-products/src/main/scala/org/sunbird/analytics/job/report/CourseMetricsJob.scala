package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.util.ESUtil
import org.sunbird.cloud.storage.conf.AppConf

case class ESIndexResponse(isOldIndexDeleted: Boolean, isIndexLinkedToAlias: Boolean)

case class BatchDetails(batchid: String, courseCompletionCountPerBatch: Long, participantsCountPerBatch: Long)

case class CourseBatch(batchid: String, startDate: String, endDate: String, courseChannel: String);

trait ReportGenerator {

  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame

  def prepareReport(spark: SparkSession, storageConfig: StorageConfig, fetchTable: (SparkSession, Map[String, String]) => DataFrame, config: JobConfig)(implicit fc: FrameworkContext): Unit
}

object CourseMetricsJob extends optional.Application with IJob with ReportGenerator with BaseReportsJob {

  implicit val className = "org.ekstep.analytics.job.CourseMetricsJob"
  val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
  val sunbirdCoursesKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
  val metrics = scala.collection.mutable.Map[String, BigInt]();

  def name(): String = "CourseMetricsJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {

    JobLogger.init("CourseMetricsJob")
    JobLogger.start("CourseMetrics Job Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig);

    implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig);
    implicit val frameworkContext = getReportingFrameworkContext();
    execute(jobConfig)
  }

  private def execute(config: JobConfig)(implicit sc: SparkContext, fc: FrameworkContext) = {
    val tempDir = AppConf.getConfig("course.metrics.temp.dir")
    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val renamedDir = s"$tempDir/renamed"
    val sparkConf = sc.getConf
      .set("es.write.operation", "upsert")
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)

    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("course.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey)
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val time = CommonUtil.time({
      prepareReport(spark, storageConfig, loadData,config)
    });
    metrics.put("totalExecutionTime", time._1);
    JobLogger.end("CourseMetrics Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "metrics" -> metrics)))

  }

  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame = {
    spark.read.format("org.apache.spark.sql.cassandra").options(settings).load()
  }

  def getActiveBatches(loadData: (SparkSession, Map[String, String]) => DataFrame, druidQuery: DruidQueryModel)(implicit spark: SparkSession, fc: FrameworkContext): Array[Row] = {
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val courseBatchDF = loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .select("courseid", "batchid", "enddate", "startdate")
    val timestamp = DateTime.now().minusDays(1).withTimeAtStartOfDay()

    /**
      * 1. Fetching course_id and channel from content-model-snapshot
      * 2. Mapping it with course_batch details
      */
    val druidResult = DruidDataFetcher.getDruidData(druidQuery)
    val finalResult = druidResult.map{f => JSONUtils.deserialize[druidOutput](f)}
    val finalDF = finalResult.toDF()

    val courseBatchDenormDF = courseBatchDF.join(finalDF, courseBatchDF.col("courseid") === finalDF.col("identifier"), "left_outer")
      .select(courseBatchDF.col("*"), finalDF.col("channel"))
    JobLogger.log("Filtering out inactive batches where date is >= " + timestamp, None, INFO)

    val activeBatches = courseBatchDenormDF.filter(col("endDate").isNull || unix_timestamp(to_date(col("endDate"), "yyyy-MM-dd")).geq(timestamp.getMillis / 1000))
    val activeBatchList = activeBatches.select("batchId", "startDate", "endDate", "channel").collect();

    JobLogger.log("Total number of active batches:" + activeBatchList.size, None, INFO)
    activeBatchList;
  }

  def recordTime[R](block: => R, msg: String): (R) = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    JobLogger.log(msg + (t1 - t0), None, INFO)
    result;
  }

  def prepareReport(spark: SparkSession, storageConfig: StorageConfig, loadData: (SparkSession, Map[String, String]) => DataFrame, config: JobConfig)(implicit fc: FrameworkContext): Unit = {

    implicit val sparkSession = spark;
    //druid Query for fetching course_id and channel
    val druidQuery = JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(config.modelParams.get("druidConfig")))
    val activeBatches = getActiveBatches(loadData, druidQuery);
    val newIndexPrefix = AppConf.getConfig("course.metrics.es.index.cbatchstats.prefix")
    val newIndex = suffixDate(newIndexPrefix)
    val userData = CommonUtil.time({
      recordTime(getUserData(loadData), "Time taken to get generate the userData- ")
    });
    val activeBatchesCount = activeBatches.size;
    metrics.put("userDFLoadTime", userData._1)
    metrics.put("activeBatchesCount", activeBatchesCount)
    for (index <- activeBatches.indices) {
      val row = activeBatches(index);
      val batch = CourseBatch(row.getString(0), row.getString(1), row.getString(2), row.getString(3));
      val result = CommonUtil.time({
        val reportDF = recordTime(getReportDF(batch, userData._2, loadData), s"Time taken to generate DF for batch ${batch.batchid} - ");
        val totalRecords = reportDF.count()
        recordTime(saveReportToBlobStore(batch, reportDF, storageConfig, totalRecords), s"Time taken to save report in blobstore for batch ${batch.batchid} - ");
        recordTime(saveReportToES(batch, reportDF, newIndex, totalRecords), s"Time taken to save report in ES for batch ${batch.batchid} - ");
        reportDF.unpersist(true);
      });
      JobLogger.log(s"Time taken to generate report for batch ${batch.batchid} is ${result._1}. Remaining batches - ${activeBatchesCount - index + 1}", None, INFO)
      createESIndex(newIndex)
    }
    userData._2.unpersist(true);

  }

  def getUserData(loadData: (SparkSession, Map[String, String]) => DataFrame)(implicit spark: SparkSession): DataFrame = {

    val userDF = loadData(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .select(col("userid"),
        col("maskedemail"),
        col("firstname"),
        col("lastname"),
        col("maskedphone"),
        col("rootorgid"),
        col("locationids"),
        col("channel"),
        col("locationids")
      ).cache()

    val userOrgDF = loadData(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .filter(lower(col("isdeleted")) === "false")
      .select(col("userid"), col("organisationid")).cache()

    val organisationDF = loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .select(col("id"), col("orgname"),
        col("channel"), col("orgcode"),
        col("locationids"), col("isrootorg")).cache()
    val locationDF = loadData(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .select(col("id"), col("name"), col("type"))

    val externalIdentityDF = loadData(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .select(col("provider"), col("idtype"), col("externalid"), col("userid")).cache()
    /**
      * externalIdMapDF - Filter out the external id by idType and provider and Mapping userId and externalId
      *
      * For state user
      * USR_EXTERNAL_IDENTITY.provider=User.channel and USR_EXTERNAL_IDENTITY.idType=USER.channel and fetch the USR_EXTERNAL_IDENTITY.id
      *
      * For Cust User
      * USR_EXTERNAL_IDENTITY.idType='declared-ext-id' and USR_EXTERNAL_IDENTITY.provider=ORG.channel
      * fetch USR_EXTERNAL_IDENTITY.id and map with USER.userid
      * For cust- How to map provider with ORG.channel
      */
    val externalIdByStateDF = userDF
      .join(externalIdentityDF, externalIdentityDF.col("idtype") === userDF.col("channel")
        && externalIdentityDF.col("provider") === userDF.col("channel")
        && externalIdentityDF.col("userid") === userDF.col("userid"), "inner")
      .select(externalIdentityDF.col("externalid"), externalIdentityDF.col("userid"),
        externalIdentityDF.col("provider"), externalIdentityDF.col("idtype"))
    val externalIdByUserDF = externalIdentityDF
      .join(organisationDF, externalIdentityDF.col("provider") === organisationDF.col("channel")
        && externalIdentityDF.col("idtype").equalTo("declared-ext-id"), "inner")
      .select(externalIdentityDF.col("externalid"), externalIdentityDF.col("userid"),
        externalIdentityDF.col("provider"), externalIdentityDF.col("idtype"))
    val externalIdMapDF = externalIdByStateDF.union(externalIdByUserDF)
    /*
    * userDenormDF lacks organisation details, here we are mapping each users to get the organisationids
    * */
    val userRootOrgDF = userDF
      .join(userOrgDF, userOrgDF.col("userid") === userDF.col("userid") && userOrgDF.col("organisationid") === userDF.col("rootorgid"))
      .select(userDF.col("*"), col("organisationid"))

    val userSubOrgDF = userDF
      .join(userOrgDF, userOrgDF.col("userid") === userDF.col("userid") && userOrgDF.col("organisationid") =!= userDF.col("rootorgid"))
      .select(userDF.col("*"), col("organisationid"))

    val rootOnlyOrgDF = userRootOrgDF
      .join(userSubOrgDF, Seq("userid"), "leftanti")
      .select(userRootOrgDF.col("*"))

    val userOrgDenormDF = rootOnlyOrgDF.union(userSubOrgDF)

    val locationDenormDF = userOrgDenormDF
      .withColumn("exploded_location", explode(col("locationids")))
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "district")
      .select(col("name").as("district_name"), col("userid"))

    /**
      * Resolve the block name by filtering location type = "BLOCK" for the locationids
      */
    val blockDenormDF = userOrgDenormDF
      .withColumn("exploded_location", explode(col("locationids")))
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "block")
      .select(col("name").as("block_name"), col("userid"))

    val userLocationResolvedDF = userOrgDenormDF
      .join(locationDenormDF, Seq("userid"), "left_outer")

    /**
      * Resolve the state name:
      * 1. State-Users:
      * ORGANISATION.locationids=LOCATION.id and LOCATION.type='state'
      * TO-DO: How to map it to the user table to get the respective user
      *
      * 2. Custodian User
      * USER.locationids=LOCATION.id and LOCATION.type='state' and fetch the name,userid
      */

    val stateInfoByUser = userDF.withColumn("explode_location", explode(col("locationids")))
      .join(locationDF, col("explode_location") === locationDF.col("id")
        && locationDF.col("type") === "state")
      .select(col("name").as("state_name"), col("userid"))

    val locationInfoByOrg = organisationDF.withColumn("explode_location", explode(col("locationids")))
      .join(locationDF, col("explode_location") === locationDF.col("id")
        && locationDF.col("type") === "state")
      .select(organisationDF.col("id"), locationDF.col("name").as("state_name"),
        organisationDF.col("isrootorg"))

    val stateInfoByOrg = locationInfoByOrg.join(userDF,
      locationInfoByOrg.col("id") === userDF.col("rootorgid") &&
        locationInfoByOrg.col("isrootorg").equalTo(true))
      .select(col("state_name"),
        userDF.col("userid"))

    val resolvedStateDenormDF = stateInfoByUser.union(stateInfoByOrg)
    val userBlockResolvedDF = userLocationResolvedDF.join(blockDenormDF, Seq("userid"), "left_outer")
    val userStateResolvedDF = userBlockResolvedDF.join(resolvedStateDenormDF, Seq("userid"), "left_outer")
    val resolvedExternalIdDF = userStateResolvedDF.join(externalIdMapDF, Seq("userid"), "left_outer")

    /*
    * Resolve organisation name from `rootorgid`
    * */

    val resolvedOrgNameDF = resolvedExternalIdDF
      .join(organisationDF, organisationDF.col("id") === resolvedExternalIdDF.col("rootorgid"), "left_outer")
      .select(resolvedExternalIdDF.col("userid"), resolvedExternalIdDF.col("rootorgid"), col("orgname").as("orgname_resolved"))

    /*
      * Resolve
      * 1. school name from `orgid`
      * 2. school UDISE code from
      *   2.1 org.orgcode if user is a state user
      *   2.2 externalID.id if user is a self signed up user
    * */

    val schoolUDISECode = resolvedExternalIdDF
      .join(organisationDF, organisationDF.col("id") === resolvedExternalIdDF.col("organisationid"), "left_outer")
      .select(resolvedExternalIdDF.col("userid"),
        resolvedExternalIdDF.col("organisationid"), col("orgcode").as("schoolUDISE_resolved"))

    val schoolNameDF = resolvedExternalIdDF
      .join(organisationDF, organisationDF.col("id") === resolvedExternalIdDF.col("organisationid"), "left_outer")
      .select(resolvedExternalIdDF.col("userid"),
        resolvedExternalIdDF.col("organisationid"), col("orgname").as("schoolname_resolved"))


    /**
      * If the user present in the multiple organisation, then zip all the org names.
      * Example:
      * FROM:
      * +-------+------------------------------------+
      * |userid |orgname                             |
      * +-------+------------------------------------+
      * |user030|SACRED HEART(B)PS,TIRUVARANGAM      |
      * |user030| MPPS BAYYARAM                     |
      * |user001| MPPS BAYYARAM                     |
      * +-------+------------------------------------+
      * TO:
      * +-------+-------------------------------------------------------+
      * |userid |orgname                                                |
      * +-------+-------------------------------------------------------+
      * |user030|[ SACRED HEART(B)PS,TIRUVARANGAM, MPPS BAYYARAM ]      |
      * |user001| MPPS BAYYARAM                                         |
      * +-------+-------------------------------------------------------+
      * Zipping the orgnames of particular userid
      *
      *
      */
    val schoolNameIndexDF = schoolNameDF.withColumn("index", count("userid").over(Window.partitionBy("userid").orderBy("userid")).cast("int"))

    val resolvedSchoolNameDF = schoolNameIndexDF.selectExpr("*").filter(col("index") === 1).drop("organisationid", "index", "batchid")
      .union(schoolNameIndexDF.filter(col("index") =!= 1).groupBy("userid").agg(collect_list("schoolname_resolved").cast("string").as("schoolname_resolved")))

    val resolvedSchoolInfoDF = resolvedSchoolNameDF.join(schoolUDISECode, Seq("userid"), "left_outer")

    resolvedExternalIdDF
      .join(resolvedSchoolInfoDF, Seq("userid"), "left_outer")
      .join(resolvedOrgNameDF, Seq("userid", "rootorgid"), "left_outer")
      .dropDuplicates("userid")
      .cache();
  }

  def getReportDF(batch: CourseBatch, userDF: DataFrame, loadData: (SparkSession, Map[String, String]) => DataFrame)(implicit spark: SparkSession): DataFrame = {

    JobLogger.log("Creating report for batch " + batch.batchid, None, INFO)
    val userCourseDenormDF = loadData(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .select(col("batchid"), col("userid"), col("courseid"), col("active"), col("certificates")
        , col("completionpercentage"), col("enrolleddate"), col("completedon")
      )
      /*
       * courseBatchDF has details about the course and batch details for which we have to prepare the report
       * courseBatchDF is the primary source for the report
       * userCourseDF has details about the user details enrolled for a particular course/batch
       */
      .where(col("batchid") === batch.batchid && lower(col("active")).equalTo("true"))
      .withColumn("enddate", lit(batch.endDate))
      .withColumn("startdate", lit(batch.startDate))
      .withColumn("course_channel", lit(batch.courseChannel))
      .withColumn(
        "course_completion",
        when(col("completionpercentage").isNull, 0)
          .when(col("completionpercentage") > 100, 100)
          .otherwise(col("completionpercentage")).cast("int"))
      .withColumn("generatedOn", date_format(from_utc_timestamp(current_timestamp.cast(DataTypes.TimestampType), "Asia/Kolkata"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .withColumn("certificate_status", when(col("certificates").isNotNull && size(col("certificates").cast("array<map<string, string>>")) > 0, "Issued").otherwise(""))
      .select(
        col("batchid"),
        col("userid"),
        col("completionpercentage"),
        col("enddate"),
        col("startdate"),
        col("enrolleddate"),
        col("completedon"),
        col("active"),
        col("courseid"),
        col("course_completion"),
        col("generatedOn"),
        col("certificate_status"),
        col("course_channel")
      )
    // userCourseDenormDF lacks some of the user information that need to be part of the report here, it will add some more user details
    val reportDF = userCourseDenormDF
      .join(userDF, Seq("userid"), "inner")
      .withColumn("resolved_externalid", when(userCourseDenormDF.col("course_channel") === userDF.col("channel"), userDF.col("externalid")).otherwise(""))
      .withColumn("resolved_schoolname", when(userCourseDenormDF.col("course_channel") === userDF.col("channel"), userDF.col("schoolname_resolved")).otherwise(""))
      .withColumn("resolved_blockname", when(userCourseDenormDF.col("course_channel") === userDF.col("channel"), userDF.col("block_name")).otherwise(""))
      .withColumn("resolved_udisecode", when(userCourseDenormDF.col("course_channel") === userDF.col("channel"), userDF.col("schoolUDISE_resolved")).otherwise(""))
      .select(
        userCourseDenormDF.col("*"),
        col("firstname"),
        col("channel"),
        col("lastname"),
        col("maskedemail"),
        col("maskedphone"),
        col("rootorgid"),
        col("userid"),
        col("locationids"),
        col("resolved_externalid"),
        col("orgname_resolved"),
        col("resolved_schoolname"),
        col("district_name"),
        col("resolved_blockname"),
        col("resolved_udisecode"),
        col("state_name"))
      .cache()
    reportDF;
  }

  def createESIndex(newIndex: String): String = {
    val cBatchIndex = AppConf.getConfig("course.metrics.es.index.cbatch")
    val aliasName = AppConf.getConfig("course.metrics.es.alias")
    try {
      val indexList = ESUtil.getIndexName(aliasName)
      val oldIndex = indexList.mkString("")
      if (!oldIndex.equals(newIndex)) ESUtil.rolloverIndex(newIndex, aliasName)
    } catch {
      case ex: Exception => {
        JobLogger.log(ex.getMessage, None, ERROR)
        ex.printStackTrace()
      }
    }
    newIndex;
  }

  def saveReportToES(batch: CourseBatch, reportDF: DataFrame, newIndex: String, totalRecords:Long)(implicit spark: SparkSession): Unit = {

    import org.elasticsearch.spark.sql._
    val participantsCount = reportDF.count()
    val courseCompletionCount = reportDF.filter(col("course_completion").equalTo(100)).count()

    val batchStatsDF = reportDF
      .select(
        concat_ws(" ", col("firstname"), col("lastname")).as("name"),
        concat_ws(":", col("userid"), col("batchid")).as("id"),
        col("userid").as("userId"),
        col("completedon").as("completedOn"),
        col("maskedemail").as("maskedEmail"),
        col("maskedphone").as("maskedPhone"),
        col("orgname_resolved").as("rootOrgName"),
        col("resolved_schoolname").as("subOrgName"),
        col("startdate").as("startDate"),
        col("enddate").as("endDate"),
        col("courseid").as("courseId"),
        col("generatedOn").as("lastUpdatedOn"),
        col("batchid").as("batchId"),
        col("course_completion").cast("long").as("completedPercent"),
        col("district_name").as("districtName"),
        col("resolved_blockname").as("blockName"),
        col("resolved_externalid").as("externalId"),
        col("resolved_udisecode").as("subOrgUDISECode"),
        col("state_name").as("StateName"),
        from_unixtime(unix_timestamp(col("enrolleddate"), "yyyy-MM-dd HH:mm:ss:SSSZ"), "yyyy-MM-dd'T'HH:mm:ss'Z'").as("enrolledOn"),
        col("certificate_status").as("certificateStatus"))

    import spark.implicits._
    val batchDetails = Seq(BatchDetails(batch.batchid, courseCompletionCount, participantsCount)).toDF
    val batchDetailsDF = batchDetails
      .withColumn("generatedOn", date_format(from_utc_timestamp(current_timestamp.cast(DataTypes.TimestampType), "Asia/Kolkata"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .select(
        col("batchid").as("id"),
        col("generatedOn").as("reportUpdatedOn"),
        when(col("courseCompletionCountPerBatch").isNull, 0).otherwise(col("courseCompletionCountPerBatch")).as("completedCount"),
        when(col("participantsCountPerBatch").isNull, 0).otherwise(col("participantsCountPerBatch")).as("participantCount"))

    val cBatchIndex = AppConf.getConfig("course.metrics.es.index.cbatch")

    try {
      batchStatsDF.saveToEs(s"$newIndex/_doc", Map("es.mapping.id" -> "id"))
      JobLogger.log("Indexing batchStatsDF is success for: " + batch.batchid, None, INFO)
      // upsert batch details to cbatch index
      batchDetailsDF.saveToEs(s"$cBatchIndex/_doc", Map("es.mapping.id" -> "id", "es.write.operation" -> "upsert"))
      JobLogger.log(s"CourseMetricsJob: Elasticsearch index stats { $cBatchIndex : { batchId: ${batch.batchid}, totalNoOfRecords: $totalRecords }}", None, INFO)

    } catch {
      case ex: Exception => {
        JobLogger.log(ex.getMessage, None, ERROR)
        ex.printStackTrace()
      }
    }
  }

  def suffixDate(index: String): String = {
    index + DateTimeFormat.forPattern("dd-MM-yyyy-HH-mm").print(DateTime.now(DateTimeZone.forID("+05:30")))
  }

  def saveReportToBlobStore(batch: CourseBatch, reportDF: DataFrame, storageConfig: StorageConfig, totalRecords:Long): Unit = {
    reportDF
      .select(
        col("resolved_externalid").as("External ID"),
        col("userid").as("User ID"),
        concat_ws(" ", col("firstname"), col("lastname")).as("User Name"),
        col("maskedemail").as("Email ID"),
        col("maskedphone").as("Mobile Number"),
        col("orgname_resolved").as("Organisation Name"),
        col("state_name").as("State Name"),
        col("district_name").as("District Name"),
        col("resolved_udisecode").as("School UDISE Code"),
        col("resolved_schoolname").as("School Name"),
        col("resolved_blockname").as("Block Name"),
        col("enrolleddate").as("Enrolment Date"),
        concat(col("course_completion").cast("string"), lit("%"))
          .as("Course Progress"),
        col("completedon").as("Completion Date"),
        col("certificate_status").as("Certificate Status"))
      .saveToBlobStore(storageConfig, "csv", "course-progress-reports/" + "report-" + batch.batchid, Option(Map("header" -> "true")), None)
    JobLogger.log(s"CourseMetricsJob: records stats before cloud upload: { batchId: ${batch.batchid}, totalNoOfRecords: $totalRecords } ", None, INFO)
  }

}