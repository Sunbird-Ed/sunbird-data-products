package org.sunbird.analytics.uci

import org.apache.spark.sql.functions.{array_contains, col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.sunbird.analytics.util.AESWrapper

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.collection.immutable.List

object UCIPrivateExhaust extends optional.Application with Serializable {

  val connProperties: Properties = CommonUtil.getPostgresConnectionProps()

  val conversationDB: String = AppConf.getConfig("uci.conversation.postgres.db")
  val conversationURL: String = AppConf.getConfig("uci.conversation.postgres.url") + s"$conversationDB"
  val conversationTable: String = AppConf.getConfig("uci.postgres.table.conversation")

  val fusionAuthDB: String = AppConf.getConfig("uci.fushionauth.postgres.db")
  val fusionAuthURL: String = AppConf.getConfig("uci.fushionauth.postgres.url") + s"$fusionAuthDB"
  val userTable: String = AppConf.getConfig("uci.postgres.table.user")
  val identityTable: String = AppConf.getConfig("uci.postgres.table.identities")
  val userRegistrationTable: String = AppConf.getConfig("uci.postgres.table.user_registration")
  val isConsentToShare = true // Default set to True

  val conversationDBC: Connection = DriverManager.getConnection(conversationURL, connProperties.getProperty("user"), connProperties.getProperty("password"));
  conversationDBC.setAutoCommit(true)
  val fashionAuthDBC: Connection = DriverManager.getConnection(fusionAuthURL, connProperties.getProperty("user"), connProperties.getProperty("password"));
  fashionAuthDBC.setAutoCommit(true)


  private val columnsOrder = List("Conversation ID", "Conversation Name", "Device ID")
  private val columnMapping = Map("applications_id" -> "Conversation ID", "name" -> "Conversation Name", "device_id" -> "Device ID")

  def processRequest(conversationId: String, tenant: String)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    val conversationDF = loadConversationData(conversationId, tenant)
    val userRegistrationDF = loadUserRegistrationTable(conversationId)
    val userDF = loadUserTable()
    val identitiesDF = loadIdentitiesTable()
    val decrypt = spark.udf.register("decrypt", decryptFn)
    val finalDF = conversationDF
      .join(userRegistrationDF, userRegistrationDF.col("applications_id") === conversationDF.col("id"), "inner")
      .join(userDF, Seq("device_id"), "inner")
      .join(identitiesDF, Seq("device_id"), "inner")
      // Decrypt the username column to get the mobile num based on the consent value
      .withColumn("device_id", when(col("consent") === true, decrypt(col("username"))).otherwise(col("device_id")))
      .select("applications_id", "name", "device_id")
    organizeDF(finalDF, columnMapping, columnsOrder)
  }

  /**
   * Fetch conversation for a specific conversation ID and Tenant
   */
  def loadConversationData(conversationId: String, tenant: String)(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    spark.read.jdbc(conversationURL, conversationTable, connProperties).select("id", "name", "owners")
      .filter(col("id") === conversationId)
      .filter(array_contains(col("owners"), tenant)) // Filtering only tenant specific report
  }

  /**
   *
   * Fetch the user Registration table data for a specific conversation ID
   */
  def loadUserRegistrationTable(conversationId: String)(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    spark.read.jdbc(fusionAuthURL, userRegistrationTable, connProperties).select("id", "applications_id")
      .filter(col("applications_id") === conversationId)
      .withColumnRenamed("id", "device_id")
  }

  /**
   * Fetch the user table data to get the consent information
   */
  def loadUserTable()(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    val consentValue = spark.udf.register("consent", getConsentValueFn)
    spark.read.jdbc(fusionAuthURL, userTable, connProperties).select("id", "data")
      .withColumnRenamed("id", "device_id")
      .withColumn("consent", consentValue(col("data")))
  }

  /**
   * Fetch the user identities table data
   * to get the mobile num by decrypting the username column based on consent
   */
  def loadIdentitiesTable()(implicit spark: SparkSession, fc: FrameworkContext): DataFrame = {
    spark.read.jdbc(fusionAuthURL,identityTable , connProperties).select("users_id", "username")
      .withColumnRenamed("users_id", "device_id")
  }

  def organizeDF(reportDF: DataFrame, finalColumnMapping: Map[String, String], finalColumnOrder: List[String]): DataFrame = {
    val fields = reportDF.schema.fieldNames
    val colNames = for (e <- fields) yield finalColumnMapping.getOrElse(e, e)
    val dynamicColumns = fields.toList.filter(e => !finalColumnMapping.keySet.contains(e))
    val columnWithOrder = (finalColumnOrder ::: dynamicColumns).distinct
    reportDF.toDF(colNames: _*).select(columnWithOrder.head, columnWithOrder.tail: _*).na.fill("")
  }

  def getConsentValueFn: String => Boolean = (device_data: String) => {
    val device = JSONUtils.deserialize[Map[String, AnyRef]](device_data)
    device.getOrElse("device", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("consent", isConsentToShare).asInstanceOf[Boolean]
  }

  def decryptFn: String => String = (encryptedValue: String) => {
   AESWrapper.decrypt(encryptedValue, Some(AppConf.getConfig("uci_encryption_secret")))
  }

}
