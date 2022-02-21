package org.sunbird.analytics.exhaust.uci

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.sunbird.analytics.util.AESWrapper

import java.util.Properties
import scala.collection.immutable.List

object UCIPrivateExhaustJob extends optional.Application with BaseUCIExhaustJob {

  val identityTable: String = AppConf.getConfig("uci.postgres.table.identities")
  val userRegistrationTable: String = AppConf.getConfig("uci.postgres.table.user_registration")

  private val columnsOrder = List("Conversation ID", "Conversation Name", "Decrypted Device ID", "Encrypted Device ID", "Device UUID")
  private val columnMapping = Map("applications_id" -> "Conversation ID", "name" -> "Conversation Name", "device_id" -> "Decrypted Device ID", "username" -> "Encrypted Device ID", "device_id_uuid" -> "Device UUID")

  /** START - Overridable Methods */
  override def jobId(): String = "uci-private-exhaust"

  override def jobName(): String = "UCIPrivateExhaust"

  override def getReportPath(): String = "uci-private-exhaust/"

  override def getReportKey(): String = "userinfo"

  override def getClassName(): String = "org.sunbird.analytics.exhaust.uci.UCIPrivateExhaustJob"

  override def process(conversationId: String, telemetryDF: DataFrame, conversationDF: DataFrame)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    val userRegistrationDF = loadUserRegistrationTable(conversationId)
    val userDF = loadUserTable()
    val identitiesDF = loadIdentitiesTable()
    val decrypt = spark.udf.register("decrypt", decryptFn)
    val finalDF = conversationDF
      .join(userRegistrationDF, userRegistrationDF.col("applications_id") === conversationDF.col("id"), "inner")
      .join(userDF, Seq("device_id"), "inner")
      .join(identitiesDF, Seq("device_id"), "inner")
      //Copying column device_id to device_id_uuid 
      .withColumn("device_id_uuid", col("device_id"))
      // Decrypt the username column to get the mobile num based on the consent value
      .withColumn("device_id", when(col("consent") === true, decrypt(col("username"))).otherwise(col("device_id")))
      .select("applications_id", "name", "device_id", "username", "device_id_uuid")
    organizeDF(finalDF, columnMapping, columnsOrder)
  }

  /**
   *
   * Fetch the user Registration table data for a specific conversation ID
   */
  def loadUserRegistrationTable(conversationId: String)(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    fetchData(fusionAuthURL, fushionAuthconnectionProps, userRegistrationTable)
      .select("users_id", "applications_id")
      .filter(col("applications_id") === conversationId)
      .withColumnRenamed("users_id", "device_id")
      .select("device_id", "applications_id")
  }

  /**
   * Fetch the user identities table data
   * to get the mobile num by decrypting the username column based on consent
   */
  def loadIdentitiesTable()(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    fetchData(fusionAuthURL, fushionAuthconnectionProps, identityTable)
      .select("users_id", "username")
      .withColumnRenamed("users_id", "device_id")
      .select("device_id", "username")
  }

  def decryptFn: String => String = (encryptedValue: String) => {
    AESWrapper.decrypt(encryptedValue.trim, Some(AppConf.getConfig("uci.encryption.secret")))
  }

  def getConsentValueFn: String => Boolean = (device_data: String) => {
    val device = JSONUtils.deserialize[Map[String, AnyRef]](device_data)
    val data = device.getOrElse("data", Map()).asInstanceOf[Map[String, AnyRef]]
    data.getOrElse("device", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("consent", isConsentToShare).asInstanceOf[Boolean]
  }

  /**
   * Fetch the user table data to get the consent information
   */
  def loadUserTable()(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    val consentValue = spark.udf.register("consent", getConsentValueFn)
    fetchData(fusionAuthURL, fushionAuthconnectionProps, userTable)
      .select("id", "data")
      .withColumnRenamed("id", "device_id")
      .withColumn("consent", consentValue(col("data")))
      .select("device_id", "data", "consent")
  }
}
