package org.sunbird.analytics.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, concat_ws, desc, lit, row_number, to_date}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, StorageConfig}
import org.ekstep.analytics.model.{MergeFiles, MergeScriptConfig, OutputConfig, ReportConfig}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.analytics.job.report.CourseReport.redisUrl
import org.sunbird.analytics.job.report.{CourseData, Level1Data}
import org.sunbird.cloud.storage.conf.AppConf

//Getting live courses from compositesearch
case class CourseDetails(result: Result)

case class Result(content: List[CourseInfo])

case class CourseInfo(channel: String, identifier: String, name: String)

case class CourseResponse(result: CourseResult, responseCode: String)

case class CourseResult(count: Int, content: List[CourseBatchInfo])

case class CourseBatchInfo(framework: String, identifier: String, name: String, channel: String, batches: List[BatchInfo])

case class BatchInfo(batchId: String, startDate: String, endDate: String, name: String)

case class UserData(userid: String, state: Option[String] = Option(""), district: Option[String] = Option(""), userchannel: Option[String] = Option(""), orgname: Option[String] = Option(""),
                    firstname: Option[String] = Option(""), lastname: Option[String] = Option(""), maskedemail: Option[String] = Option(""), maskedphone: Option[String] = Option(""),
                    block: Option[String] = Option(""), externalid: Option[String] = Option(""), schoolname: Option[String] = Option(""), schooludisecode: Option[String] = Option(""))

object UserCache {
  val userid = "userid"
  val userchannel = "userchannel"
  val firstname = "firstname"
  val lastname = "lastname"
  val maskedemail = "maskedemail"
  val maskedphone = "maskedphone"
  val state = "state"
  val district = "district"
  val block = "block"
  val externalid = "externalid"
  val schoolname = "schoolname"
  val schooludisecode = "schooludisecode"
  val orgname = "orgname"
}

trait CourseReport {
  def getCourse(config: Map[String, AnyRef])(sc: SparkContext): DataFrame

  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame

  def getCourseBatchDetails(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame

  def getTenantInfo(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame
}

object CourseUtils {

  implicit val className: String = "org.sunbird.analytics.util.CourseUtils"

  def getCourse(config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val apiURL = Constants.COMPOSITE_SEARCH_URL
    val request = JSONUtils.serialize(config.get("esConfig").get)
    val response = RestUtil.post[CourseDetails](apiURL, request).result.content
    val resRDD = sc.parallelize(response)
    resRDD.toDF("channel", "identifier", "courseName")
  }

  def loadData(spark: SparkSession, settings: Map[String, String]): DataFrame = {
    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(settings)
      .load()
  }

  def getCourseBatchDetails(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame = {
    val sunbirdCoursesKeyspace = Constants.SUNBIRD_COURSES_KEY_SPACE
    loadData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .select(
        col("courseid").as("courseId"),
        col("batchid").as("batchId"),
        col("name").as("batchName"),
        col("status").as("status")
      )
  }

  def getTenantInfo(spark: SparkSession, loadData: (SparkSession, Map[String, String]) => DataFrame): DataFrame = {
    val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
    loadData(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace)).select("slug", "id")
  }

  def postDataToBlob(data: DataFrame, outputConfig: OutputConfig, config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext) = {
    val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
    val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))

    val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
    val key = config.getOrElse("key", null).asInstanceOf[String]

    val fieldsList = data.columns
    val dimsLabels = labelsLookup.filter(x => outputConfig.dims.contains(x._1)).values.toList
    val filteredDf = data.select(fieldsList.head, fieldsList.tail: _*)
    val renamedDf = filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*).na.fill("unknown")
    val reportFinalId = if (outputConfig.label.nonEmpty && outputConfig.label.get.nonEmpty) reportConfig.id + "/" + outputConfig.label.get else reportConfig.id
    val finalDf = renamedDf.na.replace("Status", Map("0" -> BatchStatus(0).toString, "1" -> BatchStatus(1).toString, "2" -> BatchStatus(2).toString))
    saveReport(finalDf, config ++ Map("dims" -> dimsLabels, "reportId" -> reportFinalId, "fileParameters" -> outputConfig.fileParameters), reportConfig)
  }

  def saveReport(data: DataFrame, config: Map[String, AnyRef], reportConfig: ReportConfig)(implicit sc: SparkContext, fc: FrameworkContext): Unit = {
    val container = config.getOrElse("container", "test-container").toString
    val storageConfig = StorageConfig(config.getOrElse("store", "local").toString, container, config.getOrElse("filePath", "/tmp/druid-reports").toString, config.get("accountKey").asInstanceOf[Option[String]], config.get("accountSecret").asInstanceOf[Option[String]])
    val format = config.getOrElse("format", "csv").asInstanceOf[String]
    val key = config.getOrElse("key", null).asInstanceOf[String]
    val reportId = config.getOrElse("reportId", "").asInstanceOf[String]
    val fileParameters = config.getOrElse("fileParameters", List("")).asInstanceOf[List[String]]
    val dims = config.getOrElse("folderPrefix", List()).asInstanceOf[List[String]]
    val mergeConfig = reportConfig.mergeConfig
    val deltaFiles = if (dims.nonEmpty) {
      data.saveToBlobStore(storageConfig, format, reportId, Option(Map("header" -> "true")), Option(dims))
    } else {
      data.saveToBlobStore(storageConfig, format, reportId, Option(Map("header" -> "true")), None)
    }
    if (mergeConfig.nonEmpty) {
      val mergeConf = mergeConfig.get
      val reportPath = mergeConf.reportPath
      val fileList = getDeltaFileList(deltaFiles, reportId, reportPath, storageConfig)
      val mergeScriptConfig = MergeScriptConfig(reportId, mergeConf.frequency, mergeConf.basePath, mergeConf.rollup,
        mergeConf.rollupAge, mergeConf.rollupCol, mergeConf.rollupRange, MergeFiles(fileList, List("Date")), container, mergeConf.postContainer)
      mergeReport(mergeScriptConfig)
    } else {
      JobLogger.log(s"Merge report is not configured, hence skipping that step", None, INFO)
    }
  }

  def getDeltaFileList(deltaFiles: List[String], reportId: String, reportPath: String, storageConfig: StorageConfig): List[Map[String, String]] = {
    if ("content_progress_metrics".equals(reportId) || "etb_metrics".equals(reportId)) {
      deltaFiles.map { f =>
        val reportPrefix = f.split(reportId)(1)
        Map("reportPath" -> reportPrefix, "deltaPath" -> f.substring(f.indexOf(storageConfig.fileName, 0)))
      }
    } else {
      deltaFiles.map { f =>
        val reportPrefix = f.substring(0, f.lastIndexOf("/")).split(reportId)(1)
        Map("reportPath" -> (reportPrefix + "/" + reportPath), "deltaPath" -> f.substring(f.indexOf(storageConfig.fileName, 0)))
      }
    }
  }

  def mergeReport(mergeConfig: MergeScriptConfig, virtualEnvDir: Option[String] = Option("/mount/venv")): Unit = {
    val mergeConfigStr = JSONUtils.serialize(mergeConfig)
    println("merge config: " + mergeConfigStr)
    val mergeReportCommand = Seq("bash", "-c",
      s"source ${virtualEnvDir.get}/bin/activate; " +
        s"dataproducts report_merger --report_config='$mergeConfigStr'")
    JobLogger.log(s"Merge report script command:: $mergeReportCommand", None, INFO)
    val mergeReportExitCode = ScriptDispatcher.dispatch(mergeReportCommand)
    if (mergeReportExitCode == 0) {
      JobLogger.log(s"Merge report script::Success", None, INFO)
    } else {
      JobLogger.log(s"Merge report script failed with exit code $mergeReportExitCode", None, ERROR)
      throw new Exception(s"Merge report script failed with exit code $mergeReportExitCode")
    }
  }

  def getCourseInfo(spark: SparkSession, courseId: String): CourseBatchInfo = {
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    val apiUrl = Constants.COMPOSITE_SEARCH_URL
    val request =
      s"""{
         |	"request": {
         |		"filters": {
         |      "identifier": "$courseId"
         |		},
         |		"sort_by": {
         |			"createdOn": "desc"
         |		},
         |		"limit": 10000,
         |		"fields": ["framework", "identifier", "name", "channel", "batches"]
         |	}
         |}""".stripMargin
    val response = RestUtil.post[CourseResponse](apiUrl, request)
    if (null != response && response.responseCode.equalsIgnoreCase("ok") && null != response.result.content && response.result.content.nonEmpty) {
      response.result.content.head
    } else CourseBatchInfo("", "", "", "", List())
  }

  def filterContents(query: String): List[CourseBatchInfo] = {
    val apiUrl = Constants.COMPOSITE_SEARCH_URL
    val response = RestUtil.post[CourseResponse](apiUrl, query)
    if (null != response && response.responseCode.equalsIgnoreCase("ok") && null != response.result.content && response.result.content.nonEmpty) {
      response.result.content
    } else List[CourseBatchInfo]()
  }

  def getActiveBatches(loadData: (SparkSession, Map[String, String], String, StructType, Option[Seq[String]]) => DataFrame, url: String, batchList: List[String], sunbirdCoursesKeyspace: String)
                      (implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    implicit val sqlContext: SQLContext = spark.sqlContext
    val courseBatchDF = loadCourseBatch(loadData, url, batchList, sunbirdCoursesKeyspace)
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val comparisonDate = fmt.print(DateTime.now(DateTimeZone.UTC).minusDays(1))
    JobLogger.log("Filtering out inactive batches where date is >= " + comparisonDate, None, INFO)

    val activeBatches = courseBatchDF.filter(col("enddate").isNull || to_date(col("enddate"), "yyyy-MM-dd").geq(lit(comparisonDate)))
    val activeBatchList = activeBatches.toDF()
    JobLogger.log("Total number of active batches:" + activeBatchList.count(), None, INFO)
    courseBatchDF.unpersist(true)
    activeBatchList
  }

  def loadCourseBatch(fetchData: (SparkSession, Map[String, String], String, StructType, Option[Seq[String]]) => DataFrame, url: String, batchList: List[String], sunbirdCoursesKeyspace: String)
                     (implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {

    implicit val sqlContext: SQLContext = spark.sqlContext
    val courseBatchDF = if (batchList.nonEmpty) {
      fetchData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace), url, new StructType(), Some(Seq("courseid", "batchid", "enddate", "startdate")))
        .filter(batch => batchList.contains(batch.getString(1)))
        .persist(StorageLevel.MEMORY_ONLY)
    }
    else {
      fetchData(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace), url, new StructType(), Some(Seq("courseid", "batchid", "enddate", "startdate")))
        .persist(StorageLevel.MEMORY_ONLY)
    }
    courseBatchDF

  }

//  def getUserData(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, Option[StructType], Option[Seq[String]]) => DataFrame): DataFrame = {
//    val schema = Encoders.product[UserData].schema
//    fetchData(spark, Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid"), "org.apache.spark.sql.redis", Some(schema), Some(Seq("firstname", "lastname", "userid", "state", "district", "userchannel", "orgname", "maskedemail", "maskedphone", "block", "externalid", "schoolname", "schooludisecode")))
//      .withColumn("username", concat_ws(" ", col("firstname"), col("lastname"))).persist(StorageLevel.MEMORY_ONLY)
//  }

  def getFilteredBatches(spark: SparkSession, activeBatches: DataFrame, config: JobConfig): Array[Row] = {
    implicit val sparkSession: SparkSession = spark
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    val modelParams = config.modelParams.get
    val contentFilters = modelParams.getOrElse("contentFilters", Map()).asInstanceOf[Map[String, AnyRef]]
    val filteredBatches = if (contentFilters.nonEmpty) {
      val filteredContents = CourseUtils.filterContents(JSONUtils.serialize(contentFilters)).toDF()
      activeBatches.join(filteredContents, activeBatches.col("courseid") === filteredContents.col("identifier"), "inner")
        .select(activeBatches.col("*")).collect
    } else activeBatches.collect
    filteredBatches
  }

  def recordTime[R](block: => R, msg: String): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    JobLogger.log(msg + (t1 - t0), None, INFO)
    result
  }

  def getContentNames(spark: SparkSession, content: List[String], contentType: String): DataFrame = {
    implicit val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._
    var assessmentInfo: List[CourseBatchInfo] = List()
    if (content.nonEmpty) {
      val apiUrl = Constants.COMPOSITE_SEARCH_URL
      val contentList = JSONUtils.serialize(content)
      val request =
        s"""
           |{
           |  "request": {
           |    "filters": {
           |      "contentType": "$contentType",
           |       "identifier": $contentList
           |    },
           |    "sort_by": {"createdOn":"desc"},
           |    "limit": 10000,
           |    "fields": ["name","identifer","contentType"]
           |  }
           |}
       """.stripMargin
      val response = RestUtil.post[CourseResponse](apiUrl, request)
      assessmentInfo = if (null != response && response.responseCode.equalsIgnoreCase("ok") && response.result.count > 0) {
        response.result.content
      } else List()
    }
    assessmentInfo.toDF().select("name", "identifier")
  }

  def filterAssessmentDF(assesmentDF: DataFrame): DataFrame = {
    val bestScoreReport = AppConf.getConfig("assessment.metrics.bestscore.report").toBoolean
    val columnName: String = if (bestScoreReport) "total_score" else "last_attempted_on"
    val df = Window.partitionBy("userid", "batchid", "courseid", "content_id").orderBy(desc(columnName))
    assesmentDF.withColumn("rownum", row_number.over(df)).where(col("rownum") === 1).drop("rownum").persist(StorageLevel.MEMORY_ONLY)
  }

  def getUserData(spark: SparkSession, loadData: (SparkSession, Map[String, String], String, StructType, Option[Seq[String]]) => DataFrame, cols:Seq[String]) = {
    val schema = Encoders.product[UserData].schema
    loadData(spark, Map("table" -> "user", "infer.schema" -> "true", "key.column" -> "userid"), redisUrl, schema,
      Some(cols)).persist(StorageLevel.MEMORY_ONLY)
      .withColumn("user_name", concat_ws(" ", col("firstname"), col("lastname")))
  }

  def parseCourseHierarchy(data: List[Map[String, AnyRef]], levelCount: Int, prevData: CourseData, depthLevel: Int): CourseData = {
    if (levelCount < depthLevel) {
      val list = data.map(childNodes => {
        val mimeType = childNodes.getOrElse("mimeType", "").asInstanceOf[String]
        val visibility = childNodes.getOrElse("visibility", "").asInstanceOf[String]
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