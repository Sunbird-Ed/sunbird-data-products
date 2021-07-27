package org.sunbird.analytics.exhaust.collection

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig}
import org.sunbird.analytics.exhaust.JobRequest

object UserInfoExhaustJob extends optional.Application with BaseCollectionExhaustJob with Serializable {

  override def getClassName = "org.sunbird.analytics.exhaust.collection.UserInfoExhaustJob"
  override def jobName() = "UserInfoExhaustJob";
  override def jobId() = "userinfo-exhaust";
  override def getReportPath() = "userinfo-exhaust/";
  override def getReportKey() = "userinfo";
  private val encryptedFields = Array("email", "phone");

  override def getUserCacheColumns(): Seq[String] = {
    Seq("userid", "username", "state", "district", "rootorgid", "orgname", "email", "phone", "block", "cluster", "usertype", "usersubtype", "schooludisecode", "schoolname")
  }

  override def validateRequest(request: JobRequest): Boolean = {
    if (super.validateRequest(request)) {
      if (request.encryption_key.isDefined) true else false;
    } else {
      false;
    }
  }

  private val filterColumns = Seq("courseid", "collectionName", "batchid", "batchName", "userid", "username", "state", "district", "orgname", "email", "phone",
    "consentflag", "consentprovideddate","block", "cluster", "usertype", "usersubtype", "schooludisecode", "schoolname");

  private val consentFields = List("email", "phone")
  private val orgDerivedFields = List("username")
  private val columnsOrder = List("Collection Id", "Collection Name", "Batch Id", "Batch Name", "User UUID", "User Name", "User Type", "User Sub Type", "State", "District","Block", "Cluster", "School Id", "School Name", "Org Name",
    "Email ID", "Mobile Number", "Consent Provided", "Consent Provided Date");
  val columnMapping = Map("courseid" -> "Collection Id", "collectionName" -> "Collection Name", "batchid" -> "Batch Id", "batchName" -> "Batch Name", "userid" -> "User UUID",
    "username" -> "User Name", "usertype" -> "User Type", "usersubtype" -> "User Sub Type", "state" -> "State", "district" -> "District", "block" -> "Block", "cluster" -> "Cluster", "orgname" -> "Org Name",
    "email" -> "Email ID", "phone" -> "Mobile Number", "consentflag" -> "Consent Provided", "consentprovideddate" -> "Consent Provided Date",  "schooludisecode" -> "School Id", "schoolname" -> "School Name")

  override def processBatch(userEnrolmentDF: DataFrame, collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

    collectionBatch.userConsent.getOrElse("No").toLowerCase() match {
      case "yes" =>
        val unmaskedDF = decryptUserInfo(applyConsentRules(collectionBatch, userEnrolmentDF))
        val reportDF = unmaskedDF.select(filterColumns.head, filterColumns.tail: _*);
        organizeDF(reportDF, columnMapping, columnsOrder)

      case _ =>
        throw new Exception("Invalid request. User info exhaust is not applicable for collections which don't request for user consent to share data")
    }
  }

  def applyConsentRules(collectionBatch: CollectionBatch, userDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val consentDF = if (collectionBatch.requestedOrgId.equals(collectionBatch.custodianOrgId)) {
      userDF.withColumn("consentflag", lit("false"));
    } else {
      val consentDF = getUserConsentDF(collectionBatch);
      val resultDF = userDF.join(consentDF, Seq("userid"), "inner")
      // Org level consent - will be updated in 3.4 to read from user_consent table
      resultDF.withColumn("orgconsentflag", when(col("rootorgid") === collectionBatch.requestedOrgId, "true").otherwise("false"))
    }
    // Issue #SB-24966: Logic to exclude users whose consentflag is false
    consentDF.filter(col("consentflag") === "true")
  }

  def decryptUserInfo(userDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val schema = userDF.schema
    val decryptFields = schema.fields.filter(field => encryptedFields.contains(field.name));
    val resultDF = decryptFields.foldLeft(userDF)((df, field) => { df.withColumn(field.name, when(col("consentflag") === "true", UDFUtils.toDecrypt(col(field.name))).otherwise(col(field.name))) })
    resultDF
  }

  /**
   * UserInfo Exhaust should be an encrypted file. So, don't ignore zip and encryption exceptions.
   * @return
   */
  override def canZipExceptionBeIgnored(): Boolean = false

}

