package org.sunbird.analytics.model.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import org.ekstep.analytics.model.ReportConfig
import org.sunbird.analytics.job.report.BaseCourseMetrics
import org.sunbird.analytics.job.report.BaseCourseMetricsOutput
import org.sunbird.analytics.util.CourseUtils

//Timespent In Mins for a course: getCoursePlays
case class CoursePlays(date: String, courseId: String, batchId: String, timespent: Option[Double] = Option(0))
case class CourseKeys(courseId: String, batchId: String)

//Final Output
case class CourseConsumptionOutput(date: String, courseName: String, batchName: String, status: String, timespent: Option[Double] = Option(0), slug: String, reportName: String) extends AlgoOutput with Output

object CourseConsumptionModel extends BaseCourseMetrics[Empty, BaseCourseMetricsOutput, CourseConsumptionOutput, CourseConsumptionOutput] with Serializable {

  implicit val className = "org.ekstep.analytics.model.CourseConsumptionModel"
  override def name: String = "CourseConsumptionModel"

  override def algorithm(events: RDD[BaseCourseMetricsOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseConsumptionOutput] = {
    implicit val sqlContext = new SQLContext(sc)

    val druidConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(config.get("reportConfig").get)).metrics.map(_.druidQuery)
    val druidResponse = DruidDataFetcher.getDruidData(druidConfig(0))
    val coursePlaysRDD = druidResponse.map{f => JSONUtils.deserialize[CoursePlays](f)}

    val courseBatchDetailsWKeys = events.map(f => (CourseKeys(f.courseId, f.batchId), f))
    val coursePlaysDetailsWKeys = coursePlaysRDD.map(f => (CourseKeys(f.courseId,f.batchId), f))

    val joinResponse = coursePlaysDetailsWKeys.leftOuterJoin(courseBatchDetailsWKeys)
    val courseConsumption = joinResponse.map{f =>
      val coursePlay = f._2._1
      val courseMetrics = f._2._2.getOrElse(BaseCourseMetricsOutput("","","","unknown","",""))
      CourseConsumptionOutput(coursePlay.date, courseMetrics.courseName, courseMetrics.batchName, courseMetrics.status, coursePlay.timespent, courseMetrics.slug, "course_usage")
    }
    courseConsumption
  }

  override def postProcess(data: RDD[CourseConsumptionOutput], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[CourseConsumptionOutput] = {
    implicit val sqlContext = new SQLContext(sc)
    if (data.count() > 0) {
      val configMap = config("reportConfig").asInstanceOf[Map[String, AnyRef]]
      val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))

      import sqlContext.implicits._
      reportConfig.output.map { f =>
          val df = data.toDF().na.fill(0L)
          CourseUtils.postDataToBlob(df, f,config)
      }
    } else {
      JobLogger.log("No data found from druid", None, Level.INFO)
    }
    data
  }

}
