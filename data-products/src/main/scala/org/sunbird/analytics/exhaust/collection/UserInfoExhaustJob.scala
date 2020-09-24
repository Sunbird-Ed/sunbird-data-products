package org.sunbird.analytics.exhaust.collection

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.JobConfig
import org.sunbird.analytics.exhaust.JobRequest

object UserInfoExhaustJob extends optional.Application with BaseCollectionExhaustJob with Serializable {

  override def getClassName = "org.sunbird.analytics.exhaust.collection.ProgressExhaustJob"
  override def jobName() = "UserInfoExhaustJob";
  override def jobId() = "userinfo-exhaust";
  override def getReportPath() = "userinfo-exhaust/";
  override def getReportKey() = "userinfo";
  private val encryptedFields = Array("email", "phone");

  override def getUserCacheColumns(): Seq[String] = {
    Seq("userid", "username", "state", "district", "externalid", "rootorgid", "email", "phone", "userinfo")
  }

  override def validateRequest(request: JobRequest): Boolean = {
    if (super.validateRequest(request)) {
      if (request.encryption_key.isDefined) true else false;
    } else {
      false;
    }
  }

  private val filterColumns = Seq("courseid", "collectionName", "batchid", "batchName", "userid", "username", "state", "district", "externalid", "email", "phone",
    "consentflag", "consentprovideddate");

  private val consentFields = List("email", "phone")
  private val orgDerivedFields = List("externalid", "username")
  private val columnsOrder = List("Collection Id", "Collection Name", "Batch Id", "Batch Name", "User UUID", "User Name", "State", "District", "External ID",
    "Email ID", "Mobile Number", "Consent Provided", "Consent Provided Date");
  val columnMapping = Map("courseid" -> "Collection Id", "collectionName" -> "Collection Name", "batchid" -> "Batch Id", "batchName" -> "Batch Name", "userid" -> "User UUID",
    "username" -> "User Name", "state" -> "State", "district" -> "District", "externalid" -> "External ID", "schooludisecode" -> "School Id", "schoolname" -> "School Name",
    "block" -> "Block Name", "email" -> "Email ID", "phone" -> "Mobile Number", "consentflag" -> "Consent Provided", "consentprovideddate" -> "Consent Provided Date")

  override def processBatch(userEnrolmentDF: DataFrame, collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {

    collectionBatch.userConsent.getOrElse("No").toLowerCase() match {
      case "yes" =>
        val userEnrolments = userEnrolmentDF
          .withColumn("userinfodata", UDFUtils.fromJSON(col("userinfo")))
          .withColumn("email", when(col("userinfodata.declared-email").isNotNull, col("userinfodata.declared-email")).otherwise(col("email")))
          .withColumn("phone", when(col("userinfodata.declared-phone").isNotNull, col("userinfodata.declared-phone")).otherwise(col("phone")))
          .drop("userinfodata")
        val unmaskedDF = decryptUserInfo(applyConsentRules(collectionBatch, userEnrolments))
        val reportDF = unmaskedDF.withColumn("persona", when(col("externalid").isNotNull && length(col("externalid")) > 0, "Teacher").otherwise("")).select(filterColumns.head, filterColumns.tail: _*);
        organizeDF(reportDF, columnMapping, columnsOrder)
      case _ =>
        throw new Exception("Invalid request. User info exhaust is not applicable for collections which don't request for user consent to share data.")
    }
  }

  def applyConsentRules(collectionBatch: CollectionBatch, userDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val consentDF = if (collectionBatch.requestedOrgId.equals(collectionBatch.custodianOrgId)) {
      userDF.withColumn("consentflag", lit("false"));
    } else {
      val consentDF = getUserConsentDF(collectionBatch);
      val resultDF = userDF.join(consentDF, Seq("userid"), "left_outer")
      // Org level consent - will be updated in 3.4 to read from user_consent table
      resultDF.withColumn("orgconsentflag", when(col("rootorgid") === collectionBatch.requestedOrgId, "true").otherwise("false"))
    }

    val consentAppliedDF = consentFields.foldLeft(consentDF)((df, column) => df.withColumn(column, when(col("consentflag") === "true", col(column)).otherwise("")));
    orgDerivedFields.foldLeft(consentAppliedDF)((df, field) => df.withColumn(field, when(col("consentflag") === "true", col(field)).when(col("orgconsentflag") === "true", col(field)).otherwise("")));
  }

  def decryptUserInfo(userDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val schema = userDF.schema
    val decryptFields = schema.fields.filter(field => encryptedFields.contains(field.name));
    val resultDF = decryptFields.foldLeft(userDF)((df, field) => { df.withColumn(field.name, when(col("consentflag") === "true", UDFUtils.toDecrypt(col(field.name))).otherwise(col(field.name))) })
    resultDF
  }

}

