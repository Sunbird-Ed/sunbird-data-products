package org.sunbird.analytics.exhaust.collection

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.JobConfig
import org.apache.spark.sql.functions._

object UserInfoExhaustJob extends optional.Application with BaseCollectionExhaustJob {

  override def getClassName = "org.sunbird.analytics.exhaust.collection.ProgressExhaustJob"
  override def jobName() = "UserInfoExhaustJob";
  override def jobId() = "userinfo-exhaust";
  override def getReportPath() = "userinfo-exhaust/";
  override def getReportKey() = "userinfo";

  override def getUserCacheColumns(): Seq[String] = {
    Seq("userid", "username", "state", "district", "orgname", "maskedemail", "maskedphone", "externalid", "schooludisecode", "schoolname", "block", "userchannel", "board")
  }

  private val filterColumns = Seq("courseid", "collectionName", "batchid", "batchName", "userid", "username", "state", "district", "persona", "orgname", "externalid", "schooludisecode", "schoolname", "block", "board", "userchannel", "maskedemail", "maskedphone", "consentFlag");
  val finalColumnMapping = Map("courseid" -> "Collection Id", "collectionName" -> "Collection Name", "batchid" -> "Batch Id", "batchName" -> "Batch Name", "userid" -> "User UUID", "username" -> "User Name", "state" -> "State", "district" -> "District",
    "persona" -> "Persona", "orgname" -> "Org Name", "externalid" -> "External ID", "schooludisecode" -> "School Id", "schoolname" -> "School Name", "block" -> "Block Name", "board" -> "Declared Board", "userchannel" -> "Declared Org", "maskedemail" -> "Email ID", "maskedphone" -> "Mobile Number", "consentFlag" -> "Consent Provided")

  override def processBatch(userEnrolmentDF: DataFrame, collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    collectionBatch.userConsent.toLowerCase() match {
      case "yes" =>
        val unmaskedDF = decryptMaskedInfo(applyConsentRules(collectionBatch, userEnrolmentDF))
        val reportDf = unmaskedDF.withColumn("persona", lit("Teacher")).select(filterColumns.head, filterColumns.tail: _*);
        val fields = reportDf.schema.fieldNames
        val colNames = for (e <- fields) yield finalColumnMapping.getOrElse(e, e)
        reportDf.toDF(colNames: _*);
      case "no" =>
        throw new Exception("Invalid request. User info exhaust is not applicable for collections which don't request for user consent to share data.")
    }
  }

}