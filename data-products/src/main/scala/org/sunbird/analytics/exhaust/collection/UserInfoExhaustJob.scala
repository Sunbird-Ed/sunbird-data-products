package org.sunbird.analytics.exhaust.collection

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.JobConfig
import org.apache.spark.sql.functions._
import org.sunbird.analytics.util.DecryptUtil

object UserInfoExhaustJob extends optional.Application with BaseCollectionExhaustJob with Serializable {

  override def getClassName = "org.sunbird.analytics.exhaust.collection.ProgressExhaustJob"
  override def jobName() = "UserInfoExhaustJob";
  override def jobId() = "userinfo-exhaust";
  override def getReportPath() = "userinfo-exhaust/";
  override def getReportKey() = "userinfo";

  override def getUserCacheColumns(): Seq[String] = {
    Seq("userid", "username", "state", "district", "orgname", "externalid", "schooludisecode", "schoolname", "block", "userchannel", "board", "rootorgid")
  }

  private val filterColumns = Seq("courseid", "collectionName", "batchid", "batchName", "userid", "username", "state", "district", "persona", "orgname", "externalid", "schooludisecode", "schoolname", "block", "board", "userchannel",  "consentFlag");
  
  private val privateFields = List("externalid","schooludisecode","schoolname","block")
  private val columnsOrder = List("Collection Id", "Collection Name", "Batch Id", "Batch Name", "User UUID", "User Name", "State", "District", "Persona", "Org Name", "External ID", "School Id", "School Name", "Block Name", "Declared Board", "Declared Org", "Consent Provided");
  val columnMapping = Map("courseid" -> "Collection Id", "collectionName" -> "Collection Name", "batchid" -> "Batch Id", "batchName" -> "Batch Name", "userid" -> "User UUID", "username" -> "User Name", "state" -> "State", "district" -> "District",
    "persona" -> "Persona", "orgname" -> "Org Name", "externalid" -> "External ID", "schooludisecode" -> "School Id", "schoolname" -> "School Name", "block" -> "Block Name", "board" -> "Declared Board", "userchannel" -> "Declared Org", "consentFlag" -> "Consent Provided")

  override def processBatch(userEnrolmentDF: DataFrame, collectionBatch: CollectionBatch)(implicit spark: SparkSession, fc: FrameworkContext, config: JobConfig): DataFrame = {
    
    collectionBatch.userConsent.toLowerCase() match {
      case "yes" =>
        val unmaskedDF = decryptUserInfo(applyConsentRules(collectionBatch, userEnrolmentDF, privateFields))
        val reportDF = unmaskedDF.withColumn("persona", when(col("externalid").isNotNull && length(col("externalid")) > 0, "Teacher").otherwise("")).select(filterColumns.head, filterColumns.tail: _*);
        organizeDF(reportDF, columnMapping, columnsOrder)
      case _ =>
        throw new Exception("Invalid request. User info exhaust is not applicable for collections which don't request for user consent to share data.")
    }
  }

}

object DecryptUDF extends Serializable {
  def toDecryptFun(str: String): String = {
    DecryptUtil.decryptData(str)
  }

  val toDecrypt = udf[String, String](toDecryptFun)
}