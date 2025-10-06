package org.sunbird.analytics.exhaust

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.conf.AppConf
import org.sunbird.analytics.exhaust.collection.UDFUtils

trait UserCacheSupport {

  protected val redisFormat: String = "org.apache.spark.sql.redis"

  protected val userCacheDBSettings: Map[String, String] = Map(
    "table" -> "user",
    "infer.schema" -> "true",
    "key.column" -> "userid"
  )

  protected val encryptedFields: Array[String] = Array("email", "phone")

  def getUserCacheColumns(): Seq[String] = {
    Seq("userid", "firstname", "lastname", "email", "orgname", "rootorgid", "usertype", "username", "cin", "fmpsid", "province", "createddate", "designation", "training_group")
  }

  def getUserCacheDF(spark: SparkSession, fetchData: (SparkSession, Map[String, String], String, StructType) => DataFrame): DataFrame = {
    val cols = getUserCacheColumns()
    val schema = spark.implicits.newProductEncoder[org.sunbird.analytics.job.report.UserCols].schema
    val df = fetchData(spark, userCacheDBSettings, redisFormat, schema)
      .withColumn("username", concat_ws(" ", col("firstname"), col("lastname")))
      .withColumn("cin", UDFUtils.extractCIN(col("profileConfig")))
      .withColumn("fmpsid", UDFUtils.extractFMPSID(col("profileConfig")))
      .withColumn("province", UDFUtils.extractProvince(col("profileConfig")))
      .withColumn("designation", UDFUtils.extractDesignation(col("profileConfig")))
      .withColumn("training_group", UDFUtils.extractTrainingGroup(col("profileConfig")))
    val selectedDF = df.select(cols.head, cols.tail: _*)
      .repartition(AppConf.getConfig("exhaust.user.parallelism").toInt, col("userid"))
    selectedDF.persist()
    selectedDF
  }

  def decryptUserInfo(userDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val schema = userDF.schema
    val decryptFields = schema.fields.filter(field => encryptedFields.contains(field.name))
    val resultDF = decryptFields.foldLeft(userDF) { (df, field) =>
      df.withColumn(field.name, org.sunbird.analytics.exhaust.collection.UDFUtils.toDecrypt(col(field.name)))
    }
    resultDF
  }
}


