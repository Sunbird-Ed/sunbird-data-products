package org.sunbird.analytics.audit

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}
import scala.collection.immutable.List
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{col, lit, when}
import org.ekstep.analytics.framework.Level.INFO
import org.sunbird.analytics.job.report.BaseReportsJob
import org.sunbird.analytics.job.report.CollectionSummaryJobV2.{getReportingFrameworkContext, openSparkSession}


case class BatchUpdaterConfig(cassandraHost: Option[String], esHost: Option[String], kpLearningBasePath: Option[String])

case class CourseBatch(courseid: String, batchid: String, startdate: Option[String], name: String, enddate: Option[String], enrollmentenddate: Option[String], enrollmenttype: String,
                       createdfor: Option[java.util.List[String]], status: Int)

case class CourseBatchMap(courseid: String, batchid: String, status: Int)

case class CourseBatchStatusMetrics(unStarted: Long, inProgress: Long, completed: Long)

object CourseBatchStatusUpdaterJob extends optional.Application with IJob with BaseReportsJob {
  implicit val className: String = "org.sunbird.analytics.audit.CourseBatchStatusUpdaterJob"
  val cassandraFormat = "org.apache.spark.sql.cassandra"
  private val collectionBatchDBSettings = Map("table" -> "course_batch", "keyspace" -> AppConf.getConfig("sunbird.courses.keyspace"), "cluster" -> "LMSCluster")

  // $COVERAGE-OFF$ Disabling scoverage for main and execute method
  override def main(config: String)(implicit sc: Option[SparkContext], fc: Option[FrameworkContext]): Unit = {
    implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
    val jobName: String = "CourseBatchStatusUpdaterJob"
    JobLogger.init(jobName)
    JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
    implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
    implicit val spark: SparkSession = openSparkSession(jobConfig)
    implicit val sc: SparkContext = spark.sparkContext
    spark.setCassandraConf("LMSCluster", CassandraConnectorConf.ConnectionHostParam.option(AppConf.getConfig("sunbird.courses.cluster.host")))
    try {
      val res = CommonUtil.time(execute(fetchData))
      JobLogger.end(s"$jobName completed execution", "SUCCESS", Option(Map(
        "time-taken" -> res._1,
        "un-started" -> res._2.unStarted,
        "in-progress" -> res._2.inProgress,
        "completed" -> res._2.completed
      )))
    } finally {
      frameworkContext.closeContext()
      spark.close()
    }

  }

  // $COVERAGE-ON Enabling the scoverage.
  def updateCourseMetadata(courseIds: List[String], collectionBatchDF: DataFrame, dateFormatter: SimpleDateFormat, config: JobConfig)(implicit sc: SparkContext): Unit = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    JobLogger.log("Indexing course data into Neo4j", Option(Map("total_courseid" -> courseIds.length)), INFO)
    courseIds.foreach(courseId => {
      val filteredRows: DataFrame = collectionBatchDF.filter(col("courseid") === courseId)
      val batches: List[Map[String, AnyRef]] = {
        if (!filteredRows.isEmpty) {
          filteredRows.collect().map(row =>
            Map[String, AnyRef]("batchId" -> row.getAs[String]("batchid"),
              "startDate" -> row.getAs[String]("startdate"),
              "endDate" -> row.getAs[String]("enddate"),
              "enrollmentEndDate" -> {
                val enrolmentDate = row.getAs[String]("enrollmentenddate")
                val enDate = row.getAs[String]("enddate")
                if ((null != enrolmentDate && enrolmentDate.nonEmpty) || (null != enDate && enDate.nonEmpty)) row.getAs[String]("enrollmentenddate")
                else {
                  val end = dateFormatter.parse(row.getAs[String]("enddate"))
                  val cal = Calendar.getInstance
                  cal.setTime(end)
                  cal.add(Calendar.DAY_OF_MONTH, -1)
                  dateFormatter.format(cal.getTime)
                }
              },
              "enrollmentType" -> row.getAs[String]("enrollmenttype"),
              "createdFor" -> row.getAs[List[String]]("createdfor"),
              "status" -> row.getAs[AnyRef]("status"))).toList
        } else {
          null
        }
      }
      val request =
        s"""
           |{
           |  "request": {
           |    "content": {
           |      "batches": $batches
           |    }
           |  }
           |}
           |""".stripMargin
      RestUtil.patch[Map[String, AnyRef]](modelParams.getOrElse("kpLearningBasePath", "localhost:8080/learning-service") + s"""/system/v3/content/update/$courseId""", request, Some(Map("content-type" -> "application/json")))
    })
  }

  def updateBatchStatus(updaterConfig: JobConfig, collectionBatchDF: DataFrame)(implicit sc: SparkContext): CourseBatchStatusMetrics = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    dateFormatter.setTimeZone(TimeZone.getTimeZone("IST"))
    val currentDate = dateFormatter.format(new Date)

    val finalDF = collectionBatchDF.withColumn("updated_status",
      when(lit(currentDate).gt(col("enddate")), 2)
        .otherwise(
          when(lit(currentDate).geq(col("startdate")), 1).otherwise(col("status"))
        ))
      .drop("status").withColumnRenamed("updated_status", "status")

    JobLogger.log(s"Writing records into database", None, INFO)

    finalDF.write.format("org.apache.spark.sql.cassandra").options(collectionBatchDBSettings ++ Map("confirm.truncate" -> "false")).mode(SaveMode.Append).save()

    if (!finalDF.isEmpty) {
      updateCourseBatchES(finalDF.filter(col("status") > 0).select("batchid", "status").collect.map(r => Map(finalDF.select("batchid", "status").columns.zip(r.toSeq): _*)), updaterConfig)

      updateCourseMetadata(finalDF.select("courseid").collect().map(_ (0)).toList.asInstanceOf[List[String]], finalDF.filter(col("status") < 2), dateFormatter, updaterConfig)
    } else {
      JobLogger.log("No records found to update the db", None, INFO)
    }
    CourseBatchStatusMetrics(finalDF.filter(col("status") === 0).count(), finalDF.filter(col("status") === 1).count(), finalDF.filter(col("status") === 2).count())
  }

  def getCollectionBatchDF(fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit spark: SparkSession): DataFrame = {
    fetchData(spark, collectionBatchDBSettings, cassandraFormat, new StructType()).select("courseid", "batchid", "startdate", "name", "enddate", "enrollmentenddate", "enrollmenttype", "createdfor", "status")
  }

  def execute(fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig, sc: SparkContext): CourseBatchStatusMetrics = {
    val collectionBatchDF = getCollectionBatchDF(fetchData).persist()
    val metrics = Map("un-started" -> collectionBatchDF.filter(col("status") === 0).count(), "in-progress" -> collectionBatchDF.filter(col("status") === 1).count(), "completed" -> collectionBatchDF.filter(col("status") === 2).count())
    JobLogger.log(s"Batch status metrics before updating the table", Option(metrics), INFO)
    val res = updateBatchStatus(config, collectionBatchDF)
    collectionBatchDF.unpersist()
    res
  }

  def updateCourseBatchES(batchList: Array[Map[String, Any]], config: JobConfig)(implicit sc: SparkContext): Unit = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    batchList.foreach(batch => {
      val body =
        s"""
           |{
           |    "doc" : {
           |        "status" : ${batch("status")}
           |    }
           |}
           |""".stripMargin

      val requestUrl = s"${
        modelParams.getOrElse("sparkElasticsearchConnectionHost", "http: //localhost:9200")
      }/course-batch/_doc/${batch("batchid")}/_update"
      RestUtil.post[Map[String, AnyRef]](requestUrl, body)
    })
    JobLogger.log("Total Batches updates in ES", Option(Map("total_batch" -> batchList.length)), INFO)
  }
}
