package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, when, _}
import org.apache.spark.sql.{DataFrame, _}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.sunbird.cloud.storage.conf.AppConf
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.StorageConfig
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.sunbird.analytics.util.DecryptUtil

case class ValidatedUserDistrictSummary(index: Int, districtName: String, blocks: Long, schools: Long, registered: Long)
case class UserStatus(id: Long, status: String)
object UnclaimedStatus extends UserStatus(0, "UNCLAIMED")
object ClaimedStatus extends UserStatus(1, "CLAIMED")
object RejectedStatus extends UserStatus(2, "REJECTED")
object FailedStatus extends UserStatus(3, "FAILED")
object MultiMatchStatus extends UserStatus(4, "MULTIMATCH")
object OrgExtIdMismatch extends UserStatus(5, "ORGEXTIDMISMATCH")
object Eligible extends UserStatus(6, "ELIGIBLE")

case class ShadowUserData(channel: String, userextid: String, addedby: String, claimedon: java.sql.Timestamp, claimstatus: Int,
                          createdon: java.sql.Timestamp, email: String, name: String, orgextid: String, processid: String,
                          phone: String, updatedon: java.sql.Timestamp, userid: String, userids: List[String], userstatus: Int)

case class UserSelfDeclared(userid: String, orgid: String, persona: String, errortype: String,
                            status: String, userinfo: Map[String, String])

// Shadow user summary in the json will have this POJO
case class UserSummary(accounts_validated: Long, accounts_rejected: Long, accounts_unclaimed: Long, accounts_failed: Long)

object StateAdminReportJob extends optional.Application with IJob with StateAdminReportHelper {

    implicit val className: String = "org.ekstep.analytics.job.StateAdminReportJob"
    
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey);
    
    def name(): String = "StateAdminReportJob"

    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {

        JobLogger.init(name())
        JobLogger.start("Started executing", Option(Map("config" -> config, "model" -> name)))
        val jobConfig = JSONUtils.deserialize[JobConfig](config)
        JobContext.parallelization = 10
        implicit val sparkSession: SparkSession = openSparkSession(jobConfig);
        implicit val frameworkContext = getReportingFrameworkContext();
        DecryptUtil.initialise();
        execute(jobConfig)
        closeSparkSession()
    }
    
    private def execute(config: JobConfig)(implicit sparkSession: SparkSession, fc: FrameworkContext) = {

        generateReport();
        JobLogger.end("StateAdminReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
    
        val resultDf = generateExternalIdReport();
        JobLogger.end("ExternalIdReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
        generateSelfUserDeclaredZip(resultDf, config)
        JobLogger.end("ExternalIdReportJob zip completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
    }
    
    def generateExternalIdReport() (implicit sparkSession: SparkSession, fc: FrameworkContext) = {
        import sparkSession.implicits._
        
        val userSelfDeclaredEncoder = Encoders.product[UserSelfDeclared].schema
        //loading user_declarations table details based on declared values and location details and appending org-external-id if present
        var userSelfDeclaredDataDF = loadData(sparkSession, Map("table" -> "user_declarations", "keyspace" -> sunbirdKeyspace), Some(userSelfDeclaredEncoder))
        userSelfDeclaredDataDF = userSelfDeclaredDataDF.select(col("*"), col("userinfo").getItem("declared-email").as("declared-email"), col("userinfo").getItem("declared-phone").as("declared-phone"),
            col("userinfo").getItem("declared-school-name").as("declared-school-name"), col("userinfo").getItem("declared-school-udise-code").as("declared-school-udise-code"),col("userinfo").getItem("declared-ext-id").as("declared-ext-id")).drop("userinfo");
        val locationDF = locationData()
        //to-do later check if externalid is necessary not-null check is necessary
        val orgExternalIdDf = loadOrganisationData().select("externalid","channel", "id","locationIds","orgName").filter(col("channel").isNotNull)
        userSelfDeclaredDataDF = userSelfDeclaredDataDF.join(orgExternalIdDf, userSelfDeclaredDataDF.col("orgid") === orgExternalIdDf.col("id"), "leftouter").
            select(userSelfDeclaredDataDF.col("*"), orgExternalIdDf.col("*"))
        userSelfDeclaredDataDF = userSelfDeclaredDataDF.withColumn("Diksha Sub-Org ID", when(userSelfDeclaredDataDF.col("declared-school-udise-code") === orgExternalIdDf.col("externalid"), userSelfDeclaredDataDF.col("declared-school-udise-code")).otherwise(lit("")))
        //decrypting email and phone values
        val userDecrpytedDataDF = decryptDF(userSelfDeclaredDataDF)
        //appending decrypted values to the user-external-identifier dataframe
        val userExternalDecryptData  = userSelfDeclaredDataDF.join(userDecrpytedDataDF, userSelfDeclaredDataDF.col("userid") === userDecrpytedDataDF.col("userid"), "left_outer").
            select(userSelfDeclaredDataDF.col("*"), userDecrpytedDataDF.col("decrypted-email"), userDecrpytedDataDF.col("decrypted-phone"))
    
        //loading user data with location-details based on the user's from the user-external-identifier table
        var userDf = loadData(sparkSession, Map("table" -> "user", "keyspace" -> sunbirdKeyspace), None).
            select(col(  "userid"),
                col("locationIds"),
                concat_ws(" ", col("firstname"), col("lastname")).as("Name"))
        userDf = userDf.join(userExternalDecryptData, userDf.col("userid") === userExternalDecryptData.col("userid"), "left_semi");
        val userDenormDF = userDf.withColumn("exploded_location", explode_outer(col("locationids")))
            .join(locationDF, col("exploded_location") === locationDF.col("locid") && (locationDF.col("loctype") === "district" || locationDF.col("loctype") === "state"), "left_outer")
        val userDenormLocationDF = userDenormDF.groupBy("userid", "Name").pivot("loctype").agg(first("locname").as("locname"))
        //listing out the user details with location info, if location details found in user-external-identifier else pick from user dataframe
        saveUserSelfDeclaredExternalInfo(userExternalDecryptData, userDenormLocationDF)
        
    }
    
    def generateSelfUserDeclaredZip(blockData: DataFrame, jobConfig: JobConfig): Unit = {
        val params = jobConfig.modelParams.getOrElse(Map())
        val virtualEnvDirectory = params.getOrElse("adhoc_scripts_virtualenv_dir", "/mount/venv")
        val scriptOutputDirectory = params.getOrElse("adhoc_scripts_output_dir", "/mount/portal_data")
        
        val channels = blockData.select(col("Channel")).distinct().collect.map(_.getString(0)).mkString(",")
        val userDeclaredDetailReportCommand = Seq("bash", "-c",
            s"source $virtualEnvDirectory/bin/activate; " +
                s"dataproducts user_declared_detail --data_store_location='$scriptOutputDirectory' --states='$channels'")
        JobLogger.log(s"User self-declared detail persona zip report command:: $userDeclaredDetailReportCommand", None, INFO)
        val userDeclaredDetailReportExitCode = ScriptDispatcher.dispatch(userDeclaredDetailReportCommand)
        
        if (userDeclaredDetailReportExitCode == 0) {
            JobLogger.log(s"Self-Declared user level zip generation::Success", None, INFO)
        } else {
            JobLogger.log(s"Self-Declared user level zip generation failed with exit code $userDeclaredDetailReportExitCode", None, ERROR)
            throw new Exception(s"Self-Declared user level zip generation failed with exit code $userDeclaredDetailReportExitCode")
        }
    }
    
    private def decryptDF(userExternalOriginalDataDF: DataFrame) (implicit sparkSession: SparkSession, fc: FrameworkContext) : DataFrame = {
        import sparkSession.implicits._
        val emailMap = userExternalOriginalDataDF.rdd.map(r => (r.getString(0), r.getString(3))).collectAsMap()
        val phoneMap = userExternalOriginalDataDF.rdd.map(r => (r.getString(0), r.getString(5))).collectAsMap()
        val decEmailMap = collection.mutable.Map[String, String]()
        val decPhoneMap = collection.mutable.Map[String, String]()
        emailMap.foreach(email => {
            val decEmail = DecryptUtil.decryptData(email._2)
            decEmailMap += (email._1 -> decEmail)
        })
        phoneMap.foreach(phone => {
            val decPhone = DecryptUtil.decryptData(phone._2)
            decPhoneMap += (phone._1 -> decPhone)
        })
        val decryptEmailDF = decEmailMap.toSeq.toDF("userId", "decrypted-email")
        val decryptPhoneDF = decPhoneMap.toSeq.toDF("userId", "decrypted-phone")
        val userDecrpytedDataDF = decryptEmailDF.join(decryptPhoneDF, decryptEmailDF.col("userId") === decryptPhoneDF.col("userId"), "left_outer").
            select(decryptEmailDF.col("*"), decryptPhoneDF.col("decrypted-phone"))
        userDecrpytedDataDF
    }
    
    private def saveUserSelfDeclaredExternalInfo(userExternalDecryptData: DataFrame, userDenormLocationDF: DataFrame): DataFrame ={
        val resultDf = userExternalDecryptData.join(userDenormLocationDF, userExternalDecryptData.col("userid") === userDenormLocationDF.col("userid"), "left_outer").
            select(col("Name"),
                userExternalDecryptData.col("userid").as("Diksha UUID"),
                when(userDenormLocationDF.col("state").isNotNull, userDenormLocationDF.col("state")).otherwise(lit("")).as("State"),
                when(userDenormLocationDF.col("district").isNotNull, userDenormLocationDF.col("district")).otherwise(lit("")).as("District"),
                col("declared-school-name"). as("School Name"),
                col("declared-school-udise-code").as("School UDISE ID"),
                col("declared-ext-id").as("State provided ext. ID"),
                col("decrypted-phone").as("Phone number"),
                col("decrypted-email").as("Email ID"),
                col("persona").as("Persona"),
                col("orgid").as("Organisation Id"),
                col("status").as("Status"),
                col("errortype").as("Error Type"),
                col("Diksha Sub-Org ID"),
                col("channel").as("Channel"),
                col("channel").as("provider"))
        resultDf.toDF.saveToBlobStore(storageConfig, "csv", "declared_user_detail", Option(Map("header" -> "true")), Option(Seq("provider")))
        resultDf
    }
    
    def generateStateRootSubOrgDF(subOrgDF: DataFrame, claimedShadowDataSummaryDF: DataFrame, claimedShadowUserDF: DataFrame) = {
        val rootSubOrg = subOrgDF.where(col("isrootorg") && col("status").equalTo(1))
        val stateUsersDf = rootSubOrg.join(claimedShadowUserDF, rootSubOrg.col("externalid") === (claimedShadowUserDF.col("orgextid")),"inner")
            .withColumnRenamed("orgname","School name")
            .withColumn("District id", lit("")).withColumn("District name", lit( "")).withColumn("Block id", lit("")).withColumn("Block name", lit(""))
            .select(col("School name"), col("District id"), col("District name"), col("Block id"), col("Block name"), col("slug"),
                col("externalid"),col("userextid"),col("name"),col("userid"))
        stateUsersDf
    }
    
    def generateReport()(implicit sparkSession: SparkSession, fc: FrameworkContext)   = {

        import sparkSession.implicits._

        val shadowDataEncoder = Encoders.product[ShadowUserData].schema
        val shadowUserDF = loadData(sparkSession, Map("table" -> "shadow_user", "keyspace" -> sunbirdKeyspace), Some(shadowDataEncoder)).as[ShadowUserData]
        val claimedShadowUserDF = shadowUserDF.where(col("claimstatus")=== ClaimedStatus.id)
        
        val organisationDF = loadOrganisationSlugDF()
        val channelSlugDF = getChannelSlugDF(organisationDF)
        val shadowUserStatusDF = appendShadowUserStatus(shadowUserDF);
        val shadowDataSummary = generateSummaryData(shadowUserStatusDF)
        
        saveUserSummaryReport(shadowDataSummary, channelSlugDF, storageConfig)
        saveUserDetailsReport(shadowUserStatusDF, channelSlugDF, storageConfig)

        // Only claimed used
        val claimedShadowDataSummaryDF = claimedShadowUserDF.groupBy("channel")
          .pivot("claimstatus").agg(count("claimstatus")).na.fill(0)

        
        // We can directly write to the slug folder
        val subOrgDF: DataFrame = generateSubOrgData(organisationDF)
        val blockDataWithSlug:DataFrame = generateBlockLevelData(subOrgDF)
        val userDistrictSummaryDF = claimedShadowUserDF.join(blockDataWithSlug, blockDataWithSlug.col("externalid") === (claimedShadowUserDF.col("orgextid")),"inner")
        val validatedUsersWithDst = userDistrictSummaryDF.groupBy(col("slug"), col("Channels")).agg(countDistinct("District name").as("districts"),
            countDistinct("Block id").as("blocks"), countDistinct(claimedShadowUserDF.col("orgextid")).as("schools"), count("userid").as("subOrgRegistered"))
        val validatedShadowDataSummaryDF = claimedShadowDataSummaryDF.join(validatedUsersWithDst, claimedShadowDataSummaryDF.col("channel") === validatedUsersWithDst.col("Channels"))
        val validatedGeoSummaryDF = validatedShadowDataSummaryDF.withColumn("registered",
          when(col("1").isNull, 0).otherwise(col("1"))).withColumn("rootOrgRegistered", col("registered")-col("subOrgRegistered")).drop("1", "channel", "Channels")

        saveUserValidatedSummaryReport(validatedGeoSummaryDF, storageConfig)
        val stateOrgDf = generateStateRootSubOrgDF(subOrgDF, claimedShadowDataSummaryDF, claimedShadowUserDF.toDF());
        saveValidatedUserDetailsReport(userDistrictSummaryDF, storageConfig, "validated-user-detail")
        saveValidatedUserDetailsReport(stateOrgDf, storageConfig, "validated-user-detail-state")
        
        val districtUserResult = userDistrictSummaryDF.groupBy(col("slug"), col("District name").as("districtName")).
            agg(countDistinct("Block id").as("blocks"),countDistinct(claimedShadowUserDF.col("orgextid")).as("schools"), count("userextid").as("registered"))
        saveUserDistrictSummary(districtUserResult, storageConfig)
        
        districtUserResult
    }

    def saveValidatedUserDetailsReport(userDistrictSummaryDF: DataFrame, storageConfig: StorageConfig, reportId: String) : Unit = {
      val window = Window.partitionBy("slug").orderBy(asc("District name"))
      val userDistrictDetailDF = userDistrictSummaryDF.withColumn("Sl", row_number().over(window)).select( col("Sl"), col("District name"), col("District id").as("District ext. ID"),
        col("Block name"), col("Block id").as("Block ext. ID"), col("School name"), col("externalid").as("School ext. ID"), col("name").as("Teacher name"),
        col("userextid").as("Teacher ext. ID"), col("userid").as("Teacher Diksha ID"), col("slug"))
      userDistrictDetailDF.saveToBlobStore(storageConfig, "csv", reportId, Option(Map("header" -> "true")), Option(Seq("slug")))
        JobLogger.log(s"StateAdminReportJob: ${reportId} report records count = ${userDistrictDetailDF.count()}", None, INFO)
    }

    def saveUserDistrictSummary(resultDF: DataFrame, storageConfig: StorageConfig)(implicit spark: SparkSession, fc: FrameworkContext) = {
      val window = Window.partitionBy("slug").orderBy(asc("districtName"))
      val districtSummaryDF = resultDF.withColumn("index", row_number().over(window))
      dataFrameToJsonFile(districtSummaryDF, "validated-user-summary-district", storageConfig)
        JobLogger.log(s"StateAdminReportJob: validated-user-summary-district report records count = ${districtSummaryDF.count()}", None, INFO)
    }

    private def getChannelSlugDF(organisationDF: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
      organisationDF.select(col("channel"), col("slug")).where(col("isrootorg") && col("status").===(1))
    }
    
    def appendShadowUserStatus(shadowUserDF: Dataset[ShadowUserData])(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._
        shadowUserDF.withColumn(
            "claim_status",
            when($"claimstatus" === UnclaimedStatus.id, lit(UnclaimedStatus.status))
                .when($"claimstatus" === ClaimedStatus.id, lit(ClaimedStatus.status))
                .when($"claimstatus" === FailedStatus.id, lit(FailedStatus.status))
                .when($"claimstatus" === RejectedStatus.id, lit(RejectedStatus.status))
                .when($"claimstatus" === MultiMatchStatus.id, lit(MultiMatchStatus.status))
                .when($"claimstatus" === OrgExtIdMismatch.id, lit(OrgExtIdMismatch.status))
                .when($"claimstatus" === Eligible.id, lit(Eligible.status))
                .otherwise(lit("")))
    }

    def generateSummaryData(shadowUserDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
        shadowUserDF.groupBy("channel")
          .pivot("claim_status").agg(count("claim_status")).na.fill(0)
    }

    /**
      * Saves the raw data as a .csv.
      * Appends /detail to the URL to prevent overwrites.
      * Check function definition for the exact column ordering.
      * @param reportDF
      * @param url
      */
    def saveUserDetailsReport(reportDF: DataFrame, channelSlugDF: DataFrame, storageConfig: StorageConfig) (implicit spark: SparkSession): Unit = {
        import spark.implicits._
        // List of fields available
        //channel,userextid,addedby,claimedon,claimstatus,createdon,email,name,orgextid,phone,processid,updatedon,userid,userids,userstatus
        val userDetailReport = reportDF.withColumn("shadow_user_status",
            when($"claimstatus" === UnclaimedStatus.id, lit(UnclaimedStatus.status))
            .when($"claimstatus" === ClaimedStatus.id, lit(ClaimedStatus.status))
            .when($"claimstatus" === FailedStatus.id, lit(FailedStatus.status))
            .when($"claimstatus" === RejectedStatus.id, lit(RejectedStatus.status))
            .when($"claimstatus" === MultiMatchStatus.id, lit(MultiMatchStatus.status))
            .when($"claimstatus" === OrgExtIdMismatch.id, lit(FailedStatus.status))
            .when($"claimstatus" === Eligible.id, lit(Eligible.status)))
        userDetailReport.join(channelSlugDF, reportDF.col("channel") === channelSlugDF.col("channel"), "left_outer").select(
              col("slug"),
              col("shadow_user_status"),
              col("userextid").as("User external id"),
              col("userstatus").as("User account status"),
              col("userid").as("User id"),
              concat_ws(",", col("userids")).as("Matching User ids"),
              col("claimedon").as("Claimed on"),
              col("orgextid").as("School external id"),
              col("claim_status").as("Claimed status"),
              col("createdon").as("Created on"),
              col("updatedon").as("Last updated on")).filter(col(colName ="shadow_user_status").
            isin(lit(UnclaimedStatus.status),lit(ClaimedStatus.status),lit(Eligible.status),lit(RejectedStatus.status),lit(FailedStatus.status),lit(MultiMatchStatus.status))).filter(col(colName = "slug").isNotNull)
          .saveToBlobStore(storageConfig, "csv", "user-detail", Option(Map("header" -> "true")), Option(Seq("shadow_user_status","slug")))
          
        JobLogger.log(s"StateAdminReportJob: user-details report records count = ${userDetailReport.count()}", None, INFO)
    }

    def saveUserValidatedSummaryReport(reportDF: DataFrame, storageConfig: StorageConfig): Unit = {
      reportDF.saveToBlobStore(storageConfig, "json", "validated-user-summary", None, Option(Seq("slug")))
      JobLogger.log(s"StateAdminReportJob: validated-user-summary report records count = ${reportDF.count()}", None, INFO)
    }

    def saveUserSummaryReport(reportDF: DataFrame, channelSlugDF: DataFrame, storageConfig: StorageConfig): Unit = {
        val dfColumns = reportDF.columns.toSet

        // Get claim status not in the current dataframe to add them.
        val columns: Seq[String] = Seq(
            UnclaimedStatus.status,
            ClaimedStatus.status,
            RejectedStatus.status,
            FailedStatus.status,
            MultiMatchStatus.status,
            OrgExtIdMismatch.status).filterNot(dfColumns)
        val correctedReportDF = columns.foldLeft(reportDF)((acc, col) => {
            acc.withColumn(col, lit(0))
        })
        JobLogger.log(s"columns to add in this report $columns")
        val totalSummaryDF = correctedReportDF.join(channelSlugDF, correctedReportDF.col("channel") === channelSlugDF.col("channel"), "left_outer").select(
                col("slug"),
                when(col(UnclaimedStatus.status).isNull, 0).otherwise(col(UnclaimedStatus.status)).as("accounts_unclaimed"),
                when(col(ClaimedStatus.status).isNull, 0).otherwise(col(ClaimedStatus.status)).as("accounts_validated"),
                when(col(RejectedStatus.status).isNull, 0).otherwise(col(RejectedStatus.status)).as("accounts_rejected"),
                when(col(Eligible.status).isNull, 0).otherwise(col(Eligible.status)).as("accounts_eligible"),
                when(col(MultiMatchStatus.status).isNull, 0).otherwise(col(MultiMatchStatus.status)).as("accounts_duplicate"),
                when(col(FailedStatus.status).isNull, 0).otherwise(col(FailedStatus.status)).as(FailedStatus.status),
                when(col(OrgExtIdMismatch.status).isNull, 0).otherwise(col(OrgExtIdMismatch.status)).as(OrgExtIdMismatch.status))
        totalSummaryDF.withColumn(
                "accounts_failed", col(FailedStatus.status) + col(OrgExtIdMismatch.status))
            .withColumn("total", col("accounts_failed") + col("accounts_unclaimed") + col("accounts_validated") + col("accounts_rejected")
            + col("accounts_eligible") + col("accounts_duplicate"))
            .filter(col(colName = "slug").isNotNull)
            .saveToBlobStore(storageConfig, "json", "user-summary", None, Option(Seq("slug")))
        JobLogger.log(s"StateAdminReportJob: user-summary report records count = ${correctedReportDF.count()}", None, INFO)
    }

  def dataFrameToJsonFile(dataFrame: DataFrame, reportId: String, storageConfig: StorageConfig)(implicit spark: SparkSession, fc: FrameworkContext): Unit = {

    implicit val sc = spark.sparkContext;

    dataFrame.select("slug", "index", "districtName", "blocks", "schools", "registered")
      .collect()
      .groupBy(f => f.getString(0)).map(f => {
        val summary = f._2.map(f => ValidatedUserDistrictSummary(f.getInt(1), f.getString(2), f.getLong(3), f.getLong(4), f.getLong(5)))
        val arrDistrictSummary = sc.parallelize(Array(JSONUtils.serialize(summary)), 1)
        OutputDispatcher.dispatch(StorageConfig(storageConfig.store, storageConfig.container, storageConfig.fileName + reportId + "/" + f._1 + ".json", storageConfig.accountKey, storageConfig.secretKey), arrDistrictSummary);
      })

  }

}

object StateAdminReportJobTest {
    def main(args: Array[String]): Unit = {
        StateAdminReportJob.main("""{"model":"Test"}""")
    }
}
