package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.util.CourseUtils
import org.sunbird.cloud.storage.conf.AppConf

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
      prepareReport(spark, storageConfig, loadData, config)
    });
    metrics.put("totalExecutionTime", time._1);
    JobLogger.end("CourseMetrics Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "metrics" -> metrics)))
    fc.closeContext()
  }

  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame = {
    spark.read.format("org.apache.spark.sql.cassandra").options(settings).load()
  }

  def getActiveBatches(loadData: (SparkSession, Map[String, String]) => DataFrame, batchFilters: String)(implicit spark: SparkSession, fc: FrameworkContext): Array[Row] = {
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val timestamp = DateTime.now().minusDays(1).withTimeAtStartOfDay()

    val batchInfo = CourseUtils.getFilteredBatches(spark, batchFilters)
    JobLogger.log("Filtering out inactive batches where date is >= " + timestamp, None, INFO)

    val activeBatches = batchInfo.filter(col("enddate").isNull || unix_timestamp(to_date(col("enddate"), "yyyy-MM-dd")).geq(timestamp.getMillis / 1000))
    val activeBatchList = activeBatches.select("batchid", "startdate", "enddate", "channel").collect();

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
    val batchFilters = JSONUtils.serialize(config.modelParams.get("batchFilters"))
    val activeBatches = getActiveBatches(loadData, batchFilters);
    val newIndexPrefix = AppConf.getConfig("course.metrics.es.index.cbatchstats.prefix")
    val newIndex = suffixDate(newIndexPrefix)
    val userData = CommonUtil.time({
      recordTime(getUserData(), "Time taken to get generate the userData- ")
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
        reportDF.unpersist(true);
      });
      JobLogger.log(s"Time taken to generate report for batch ${batch.batchid} is ${result._1}. Remaining batches - ${activeBatchesCount - index + 1}", None, INFO)
    }
    userData._2.unpersist(true);

  }

  def getUserData()(implicit spark: SparkSession): DataFrame = {
    spark.read.format("org.apache.spark.sql.redis")
      .option("keys.pattern", "*")
      .option("infer.schema", true).load()
      .select("userid","username","schoolname","districtname","email","orgname","schooludisecode",
        "maskedemail","statename","externalid","blockname","maskedphone")
  }

  def getReportDF(batch: CourseBatch, userDF: DataFrame, loadData: (SparkSession, Map[String, String]) => DataFrame)(implicit spark: SparkSession): DataFrame = {

    JobLogger.log("Creating report for batch " + batch.batchid, None, INFO)
    val userCourseDenormDF = loadData(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .select(col("batchid"), col("userid"), col("courseid"), col("active"), col("certificates")
        , col("completionpercentage"), col("enrolleddate"), col("completedon"))
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
      .withColumn("resolved_externalid", when(userCourseDenormDF.col("course_channel") === userDF.col("channel"), userDF.col("declared-ext-id")).otherwise(""))
      .withColumn("resolved_schoolname", when(userCourseDenormDF.col("course_channel") === userDF.col("channel"), userDF.col("declared-school-name")).otherwise(""))
      .withColumn("resolved_blockname", when(userCourseDenormDF.col("course_channel") === userDF.col("channel"), userDF.col("block_name")).otherwise(""))
      .withColumn("resolved_udisecode", when(userCourseDenormDF.col("course_channel") === userDF.col("channel"), userDF.col("declared-school-udise-code")).otherwise(""))
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
        col("resolved_udisecode"),
        col("resolved_blockname"),
        col("state_name")
      )
      .cache()
    reportDF;
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
    JobLogger.log(s"CourseMetricsJob: records stats before cloud upload: { batchId: ${batch.batchid}, totalNoOfRecords: $totalRecords }} ", None, INFO)
  }

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

  /*
  * Resolve
  * 1. school name from `orgid`
  * 2. school UDISE code from
  *   2.1 org.orgcode if user is a state user
  *   2.2 externalID.id if user is a self signed up user
* */

  /**
    * Resolve the state name:
    * 1. State-Users:
    * ORGANISATION.locationids=LOCATION.id and LOCATION.type='state'
    * TO-DO: How to map it to the user table to get the respective user
    *
    * 2. Custodian User
    * USER.locationids=LOCATION.id and LOCATION.type='state' and fetch the name,userid
    */

  def getUserSelfDeclaredDetails(userDf: DataFrame, custRootOrgId: String, externalIdentityDF: DataFrame, locationDF: DataFrame): DataFrame = {

    val filterUserIdDF = userDf.filter(col("rootorgid") === lit(custRootOrgId))
      .select("userid", "locationids")
    val userIdDF = externalIdentityDF
        .join(filterUserIdDF, Seq("userid"), "inner")
        .groupBy("userid")
        .pivot("idtype", Seq("declared-ext-id", "declared-school-name", "declared-school-udise-code"))
        .agg(first(col("externalid")))
        .na.drop("all", Seq("declared-ext-id", "declared-school-name", "declared-school-udise-code"))

    val stateInfoDF = filterUserIdDF.withColumn("explode_location", explode(col("locationids")))
      .join(locationDF, col("explode_location") === locationDF.col("id")
        && locationDF.col("type") === "state")
      .select(col("name").as("state_name"), col("userid"))

    val denormUserDF = userIdDF
      .join(stateInfoDF, Seq("userid"), "left_outer")
    denormUserDF
  }

  def getStateDeclaredDetails(userDf: DataFrame, custRootOrgId: String, externalIdentityDF: DataFrame, orgDF: DataFrame, userOrgDF: DataFrame, locationDF: DataFrame): DataFrame = {

    val stateExternalIdDF = externalIdentityDF
      .join(userDf,
        externalIdentityDF.col("idtype") === userDf.col("channel")
          && externalIdentityDF.col("provider") === userDf.col("channel")
          && externalIdentityDF.col("userid") === userDf.col("userid"), "inner")
        .select(externalIdentityDF.col("userid"), col("externalid").as("declared-ext-id"))

    val schoolInfoByState = userOrgDF.join(orgDF,
      orgDF.col("id") === userOrgDF.col("organisationid"), "left_outer")
      .select(col("userid"), col("orgname").as("declared-school-name"), col("orgcode").as("declared-school-udise-code"))

    val locationInfoDF = orgDF.withColumn("explode_location", explode(col("locationids")))
      .join(locationDF, col("explode_location") === locationDF.col("id")
        && locationDF.col("type") === "state")
      .select(orgDF.col("id"), locationDF.col("name").as("state_name"),
        orgDF.col("isrootorg"))

    val stateInfoByOrg = locationInfoDF.join(userDf,
      locationInfoDF.col("id") === userDf.col("rootorgid") &&
        locationInfoDF.col("isrootorg").equalTo(true))
      .select(col("state_name"),
        userDf.col("userid"))

    val denormStateDetailDF = getResolvedSchoolInfo(schoolInfoByState)
      .join(stateExternalIdDF, Seq("userid"), "left_outer")
      .join(stateInfoByOrg, Seq("userid"), "left_outer")
        .select(schoolInfoByState.col("userid"),
          col("declared-ext-id"),
          col("declared-school-name"),
          col("declared-school-udise-code"),
          col("state_name"))
    denormStateDetailDF
  }

  def getResolvedSchoolInfo(schoolInfoDF: DataFrame): DataFrame = {

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
    val schoolNameIndexDF = schoolInfoDF.withColumn("index", count("userid").over(Window.partitionBy("userid").orderBy("userid")).cast("int"))

    val resolvedSchoolNameDF = schoolNameIndexDF.selectExpr("*").filter(col("index") === 1).drop("organisationid", "index", "batchid")
      .union(schoolNameIndexDF.filter(col("index") =!= 1).groupBy("userid").agg(collect_list("declared-school-name").cast("string").as("declared-school-name"), collect_list("declared-school-udise-code").cast("string").as("declared-school-udise-code")))
    resolvedSchoolNameDF
  }

  def getCustodianOrgId(loadData: (SparkSession, Map[String, String]) => DataFrame)(implicit spark: SparkSession): String = {
    val systemSettingDF = loadData(spark, Map("table" -> "system_settings", "keyspace" -> sunbirdKeyspace))
      .where(col("id") === "custodianOrgId" && col("field") === "custodianOrgId")
      .select(col("value")).persist()

    systemSettingDF.select("value").first().getString(0)
  }
 }