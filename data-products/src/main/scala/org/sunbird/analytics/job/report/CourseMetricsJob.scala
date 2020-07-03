package org.sunbird.analytics.job.report

import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, unix_timestamp, _}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql._
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.util.CourseUtils
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable

case class BatchDetails(batchid: String, courseCompletionCountPerBatch: Long, participantsCountPerBatch: Long)

case class CourseBatch(batchid: String, startDate: String, endDate: String, courseChannel: String)

trait ReportGenerator {

  def loadData(spark: SparkSession, settings: Map[String, String], url: String): DataFrame

  def prepareReport(spark: SparkSession, storageConfig: StorageConfig, fetchTable: (SparkSession, Map[String, String], String) => DataFrame, config: JobConfig)(implicit fc: FrameworkContext): Unit
}

object CourseMetricsJob extends optional.Application with IJob with ReportGenerator with BaseReportsJob {

  implicit val className: String = "org.ekstep.analytics.job.CourseMetricsJob"
  val sunbirdKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
  val sunbirdCoursesKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
  val metrics: mutable.Map[String, BigInt] = mutable.Map[String, BigInt]()

  def name(): String = "CourseMetricsJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {

    JobLogger.init("CourseMetricsJob")
    JobLogger.start("CourseMetrics Job Started executing", Option(Map("config" -> config, "model" -> name)))
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    JobContext.parallelization = CommonUtil.getParallelization(jobConfig)

    implicit val sparkContext: SparkContext = getReportingSparkContext(jobConfig)
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
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
    })
    metrics.put("totalExecutionTime", time._1)
    JobLogger.end("CourseMetrics Job completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "metrics" -> metrics)))
    fc.closeContext()
  }

  def loadData(spark: SparkSession, settings: Map[String, String], url: String): DataFrame = {
    spark.read.format(url).options(settings).load()
  }

  def getActiveBatches(loadData: (SparkSession, Map[String, String], String) => DataFrame)
                      (implicit spark: SparkSession, fc: FrameworkContext): Array[Row] = {

    implicit  val sqlContext: SQLContext = spark.sqlContext
    import sqlContext.implicits._

    val courseBatchDF = loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
      .select("courseid", "batchid", "enddate", "startdate")

    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val comparisonDate = fmt.print(DateTime.now(DateTimeZone.UTC).minusDays(1))

    JobLogger.log("Filtering out inactive batches where date is >= " + comparisonDate, None, INFO)

    // val activeBatches = courseBatchDF.filter(col("endDate").isNull || unix_timestamp(to_date(col("endDate"), "yyyy-MM-dd")).geq(timestamp.getMillis / 1000))
    val activeBatches = courseBatchDF.filter(col("enddate").isNull || to_date(col("enddate"), "yyyy-MM-dd").geq(lit(comparisonDate)))
    val activeBatchList = activeBatches.select("courseid","batchid", "startdate", "enddate").collect
    JobLogger.log("Total number of active batches:" + activeBatchList.length, None, INFO)

    activeBatchList
  }

  def recordTime[R](block: => R, msg: String): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    JobLogger.log(msg + (t1 - t0), None, INFO)
    result
  }

  def prepareReport(spark: SparkSession, storageConfig: StorageConfig, loadData: (SparkSession, Map[String, String], String) => DataFrame, config: JobConfig)(implicit fc: FrameworkContext): Unit = {

    implicit val sparkSession: SparkSession = spark
    val activeBatches = getActiveBatches(loadData)
    val userData = CommonUtil.time({
      recordTime(getUserData(spark, loadData), "Time taken to get generate the userData- ")
    })
    val activeBatchesCount = new AtomicInteger(activeBatches.length)
    metrics.put("userDFLoadTime", userData._1)
    metrics.put("activeBatchesCount", activeBatchesCount.get())
    val batchFilters = JSONUtils.serialize(config.modelParams.get("batchFilters"))

    for (index <- activeBatches.indices) {
      val row = activeBatches(index)
      val courses = CourseUtils.getCourseInfo(spark, row.getString(0))
      if(courses.framework.nonEmpty && batchFilters.toLowerCase.contains(courses.framework.toLowerCase)) {
        val batch = CourseBatch(row.getString(1), row.getString(2), row.getString(3), courses.channel);
        val result = CommonUtil.time({
          val reportDF = recordTime(getReportDF(batch, userData._2, loadData), s"Time taken to generate DF for batch ${batch.batchid} - ")
          val totalRecords = reportDF.count()
          recordTime(saveReportToBlobStore(batch, reportDF, storageConfig, totalRecords), s"Time taken to save report in blobstore for batch ${batch.batchid} - ")
          reportDF.unpersist(true)
        })
        JobLogger.log(s"Time taken to generate report for batch ${batch.batchid} is ${result._1}. Remaining batches - ${activeBatchesCount.getAndDecrement()}", None, INFO)
      }
    }
    userData._2.unpersist(true)

  }

  def getUserData(spark: SparkSession, loadData: (SparkSession, Map[String, String], String) => DataFrame): DataFrame = {
    loadData(spark, Map("keys.pattern" -> "*","infer.schema" -> "true"),"org.apache.spark.sql.redis")
      .select(col("userid"),col("firstname"),col("lastname"),col("schoolname"),col("districtname"),col("email"),col("orgname"),col("schooludisecode"),
        col("maskedemail"),col("statename"),col("externalid"),col("blockname"),col("maskedphone"),
        concat_ws(" ", col("firstname"), col("lastname")).as("username"))
  }

  def generateCustodianOrgUserData(custodianOrgId: String, userDF: DataFrame, organisationDF: DataFrame,
                                   locationDF: DataFrame, externalIdentityDF: DataFrame): DataFrame = {
    /**
      * Resolve the state, district and block information for CustodianOrg Users
      * CustodianOrg Users will have state, district and block (optional) information
      */

    val userExplodedLocationDF = userDF.withColumn("exploded_location", explode_outer(col("locationids")))
      .select(col("userid"), col("exploded_location"))

    val userStateDF = userExplodedLocationDF
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "state")
      .select(userExplodedLocationDF.col("userid"), col("name").as("state_name"))

    val userDistrictDF = userExplodedLocationDF
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "district")
      .select(userExplodedLocationDF.col("userid"), col("name").as("district_name"))

    val userBlockDF = userExplodedLocationDF
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "block")
      .select(userExplodedLocationDF.col("userid"), col("name").as("block_name"))

    /**
      * Join with the userDF to get one record per user with district and block information
      */

    val custodianOrguserLocationDF = userDF.filter(col("rootorgid") === lit(custodianOrgId))
      .join(userStateDF, Seq("userid"), "inner")
      .join(userDistrictDF, Seq("userid"), "left")
      .join(userBlockDF, Seq("userid"), "left")
      .select(userDF.col("*"),
        col("state_name"),
        col("district_name"),
        col("block_name")).drop(col("locationids"))

    /**
      * Obtain the ExternalIdentity information for CustodianOrg users
      */
      // isrootorg = true

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

    val custodianUserDF = custodianOrguserLocationDF.as("userLocDF")
      .join(custodianUserPivotDF, Seq("userid"), "left")
      .select("userLocDF.*", "declared-ext-id", "declared-school-name", "declared-school-udise-code", "user_channel")
    custodianUserDF
  }

  def generateStateOrgUserData(custRootOrgId: String, userDF: DataFrame, organisationDF: DataFrame, locationDF: DataFrame,
                               externalIdentityDF: DataFrame, userOrgDF: DataFrame): DataFrame = {
    /**
      * Resolve the State, district and block information for State Users
      * State Users will either have just the state info or state, district and block info
      * Obtain the location information from the organisations table first before joining
      * with the state users
      */

    val stateOrgExplodedDF = organisationDF.withColumn("exploded_location", explode_outer(col("locationids")))
      .select(col("id"), col("exploded_location"))

    val orgStateDF = stateOrgExplodedDF.join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "state")
      .select(stateOrgExplodedDF.col("id"), col("name").as("state_name"))

    val orgDistrictDF = stateOrgExplodedDF
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "district")
      .select(stateOrgExplodedDF.col("id"), col("name").as("district_name"))

    val orgBlockDF = stateOrgExplodedDF
      .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "block")
      .select(stateOrgExplodedDF.col("id"), col("name").as("block_name"))

    val stateOrgLocationDF = organisationDF
      .join(orgStateDF, Seq("id"))
      .join(orgDistrictDF, Seq("id"), "left")
      .join(orgBlockDF, Seq("id"), "left")
      .select(organisationDF.col("id").as("orgid"), col("orgname"),
        col("orgcode"), col("isrootorg"), col("state_name"), col("district_name"), col("block_name"))

    // exclude the custodian user != custRootOrgId
    // join userDf to user_orgDF and then join with OrgDF to get orgname and orgcode ( filter isrootorg = false)

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
        subOrgDF.col("district_name"),
        subOrgDF.col("block_name")).drop(col("locationids"))

    val stateUserDF = stateUserLocationResolvedDF.as("state_user")
      .join(externalIdentityDF, externalIdentityDF.col("idtype") === col("state_user.channel")
        && externalIdentityDF.col("provider") === col("state_user.channel")
        && externalIdentityDF.col("userid") === col("state_user.userid"), "left")
      .select(col("state_user.*"), externalIdentityDF.col("externalid").as("declared-ext-id"), col("rootorgid").as("user_channel"))
    stateUserDF
  }

  def getReportDF(batch: CourseBatch, userDF: DataFrame, loadData: (SparkSession, Map[String, String], String) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    JobLogger.log("Creating report for batch " + batch.batchid, None, INFO)
    val userCourseDenormDF = loadData(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace),"org.apache.spark.sql.cassandra")
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
      .withColumn("channel", lit(batch.courseChannel))
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
        col("channel")
      )
    // userCourseDenormDF lacks some of the user information that need to be part of the report here, it will add some more user details
    val reportDF = userCourseDenormDF
      .join(userDF, Seq("userid"), "inner")
      .select(
        userCourseDenormDF.col("*"),
        col("firstname"),
        col("channel"),
        col("lastname"),
        col("maskedemail"),
        col("maskedphone"),
        col("externalid"),
        col("orgname"),
        col("schoolname"),
        col("districtname"),
        col("schooludisecode"),
        col("blockname"),
        col("statename")
      ).persist()
    reportDF
  }

  def saveReportToBlobStore(batch: CourseBatch, reportDF: DataFrame, storageConfig: StorageConfig, totalRecords:Long): Unit = {
    reportDF
      .select(
        col("externalid").as("External ID"),
        col("userid").as("User ID"),
        concat_ws(" ", col("firstname"), col("lastname")).as("User Name"),
        col("maskedemail").as("Email ID"),
        col("maskedphone").as("Mobile Number"),
        col("orgname").as("Organisation Name"),
        col("statename").as("State Name"),
        col("districtname").as("District Name"),
        col("schooludisecode").as("School UDISE Code"),
        col("schoolname").as("School Name"),
        col("blockname").as("Block Name"),
        col("enrolleddate").as("Enrolment Date"),
        concat(col("course_completion").cast("string"), lit("%"))
          .as("Course Progress"),
        col("completedon").as("Completion Date"),
        col("certificate_status").as("Certificate Status"))
      .saveToBlobStore(storageConfig, "csv", "course-progress-reports/" + "report-" + batch.batchid, Option(Map("header" -> "true")), None)
    JobLogger.log(s"CourseMetricsJob: records stats before cloud upload: { batchId: ${batch.batchid}, totalNoOfRecords: $totalRecords }} ", None, INFO)
  }

 }