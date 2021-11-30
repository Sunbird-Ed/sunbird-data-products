package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.util.CommonUtil
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.analytics.util.CourseUtils

case class BaseCourseMetricsOutput(courseName: String, batchName: String, status: String, slug: String, courseId: String, batchId: String) extends AlgoInput

trait BaseCourseMetrics[T <: AnyRef, A <: BaseCourseMetricsOutput, B <: AlgoOutput, R <: Output] extends IBatchModelTemplate[T,BaseCourseMetricsOutput,B,R]{

  override def preProcess(events: RDD[T], config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): RDD[BaseCourseMetricsOutput] = {
    CommonUtil.setStorageConf(config.getOrElse("store", "local").toString, config.get("accountKey").asInstanceOf[Option[String]], config.get("accountSecret").asInstanceOf[Option[String]])
    val readConsistencyLevel: String = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val sparkConf = sc.getConf
      .set("spark.cassandra.input.consistency.level", readConsistencyLevel)
      .set("spark.sql.caseSensitive", AppConf.getConfig(key = "spark.sql.caseSensitive"))
    implicit val spark: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    val data = getCourseMetrics(spark, config)
    val encoder = Encoders.product[BaseCourseMetricsOutput]
    data.as[BaseCourseMetricsOutput](encoder).rdd
  }

  def getCourseMetrics(spark: SparkSession, config: Map[String, AnyRef])(implicit sc: SparkContext, fc: FrameworkContext): DataFrame = {
    implicit val sqlContext = new SQLContext(sc)
    val courses = CourseUtils.getCourse(config)
    val courseBatch = CourseUtils.getCourseBatchDetails(spark, CourseUtils.loadData)
    val tenantInfo = CourseUtils.getTenantInfo(spark, CourseUtils.loadData)
    val joinCourses = courses.join(courseBatch, (courses.col("identifier") === courseBatch.col("courseId")), "inner")
    val joinWithTenant = joinCourses.join(tenantInfo, joinCourses.col("channel") === tenantInfo.col("id"), "inner")
    joinWithTenant.na.fill("unknown", Seq("slug")).select("courseName","batchName","status","slug", "courseId", "batchId")
  }
}