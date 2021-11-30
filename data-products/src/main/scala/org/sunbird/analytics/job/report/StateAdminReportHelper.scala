package org.sunbird.analytics.job.report

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.sunbird.analytics.job.report.StateAdminReportJob.locationIdList
import org.sunbird.cloud.storage.conf.AppConf

trait StateAdminReportHelper extends BaseReportsJob {
  val tempDir = AppConf.getConfig("admin.metrics.temp.dir")
  val sunbirdKeyspace = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
  val summaryDir = s"$tempDir/summary"
  val renamedDir = s"$tempDir/renamed"
  val detailDir = s"$tempDir/detail"
  
  def locationData() (implicit sparkSession: SparkSession) = {
    val locationDF = loadData(sparkSession, Map("table" -> "location", "keyspace" -> sunbirdKeyspace), None).select(
      col("id").as("locid"),
      col("code").as("loccode"),
      col("name").as("locname"),
      col("parentid").as("locparentid"),
      col("type").as("loctype")).cache()
    locationDF
  }

  def generateSubOrgData(organisationDF: DataFrame)(implicit sparkSession: SparkSession) = {
    var locationDF = locationData()
    val rootOrgs = organisationDF.select(col("id").as("rootorgjoinid"), col("channel").as("rootorgchannel"), col("slug").as("rootorgslug")).where(col("istenant") && col("status").===(1)).collect();
    val rootOrgRDD = sparkSession.sparkContext.parallelize(rootOrgs.toSeq);
    val rootOrgEncoder = Encoders.product[RootOrgData].schema
    val rootOrgDF = sparkSession.createDataFrame(rootOrgRDD, rootOrgEncoder);
    val orgWithLocationDF = organisationDF.withColumn("locationids", locationIdList(col("orglocation")))
    val subOrgDF = orgWithLocationDF
      .withColumn("explodedlocation", explode(when(size(col("locationids")).equalTo(0), array(lit(null).cast("string")))
        .otherwise(when(col("locationids").isNotNull, col("locationids"))
          .otherwise(array(lit(null).cast("string"))))))
    val activeOrgDF = subOrgDF
      .where(col("status").equalTo(1))
      .join(locationDF, subOrgDF.col("explodedlocation") === locationDF.col("locid"), "left")
      .join(rootOrgDF, subOrgDF.col("rootorgid") === rootOrgDF.col("rootorgjoinid"), "left")
    activeOrgDF
  }

  def generateBlockLevelData(subOrgJoinedDF: DataFrame)(implicit sparkSession: SparkSession) = {

    val districtDF = subOrgJoinedDF.where(col("loctype").equalTo("district")).select(col("channel"), col("slug"), col("id").as("schoolid"), col("orgname").as("schoolname"), col("locid").as("districtid"), col("locname").as("districtname"), col("externalid"));
    val blockDF = subOrgJoinedDF.where(col("loctype").equalTo("block")).select(col("id").as("schooljoinid"), col("locid").as("blockid"), col("locname").as("blockname"));
    val window = Window.partitionBy("slug").orderBy(asc("districtName"))
    val blockData = blockDF.join(districtDF, blockDF.col("schooljoinid").equalTo(districtDF.col("schoolid")), "right_outer").drop(col("schooljoinid")).coalesce(1)
      .withColumn("index", row_number().over(window)).select(
      col("index"),
      col("schoolid").as("School id"),
      col("schoolname").as("School name"),
      col("channel").as("Channels"),
      col("districtid").as("District id"),
      col("districtname").as("District name"),
      col("blockid").as("Block id"),
      col("blockname").as("Block name"),
      col("slug").as("slug"),
      col("externalid"))
    blockData.filter(col(colName = "slug").isNotNull)
  }

  def loadOrganisationSlugDF()(implicit sparkSession: SparkSession) = {
    loadOrganisationData.filter(col(colName = "slug").isNotNull).cache();
  }
  
  def loadOrganisationData()(implicit sparkSession: SparkSession) = {
    loadData(sparkSession, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace), None).select(
      col("id").as("id"),
      col("rootorgid").as("rootorgid"),
      col("channel").as("channel"),
      col("status").as("status"),
      col("orgname").as("orgname"),
      col("externalid").as("externalid"),
      col("orglocation"),
      col("istenant"),
      col("slug").as("slug")).cache();
  }

}
