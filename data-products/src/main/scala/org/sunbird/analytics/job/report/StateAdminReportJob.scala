package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, lit, when, _}
import org.apache.spark.sql.{DataFrame, _}
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.sunbird.cloud.storage.conf.AppConf
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.sunbird.analytics.util.DecryptUtil
import org.ekstep.analytics.framework.util.JSONUtils

import scala.collection.mutable.ListBuffer

case class UserSelfDeclared(userid: String, orgid: String, persona: String, errortype: String,
                            status: String, userinfo: Map[String, String])

object StateAdminReportJob extends optional.Application with IJob with StateAdminReportHelper {

    implicit val className: String = "org.ekstep.analytics.job.StateAdminReportJob"
    
    val container = AppConf.getConfig("cloud.container.reports")
    val objectKey = AppConf.getConfig("admin.metrics.cloud.objectKey")
    val storageConfig = getStorageConfig(container, objectKey);
    
    //$COVERAGE-OFF$ Disabling scoverage for main and execute method
    def name(): String = "StateAdminReportJob"

    def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {

        JobLogger.init(name())
        JobLogger.start("Started executing", Option(Map("config" -> config, "model" -> name)))
        val jobConfig = JSONUtils.deserialize[JobConfig](config)
        JobContext.parallelization = 10
        implicit val sparkSession: SparkSession = openSparkSession(jobConfig);
        implicit val frameworkContext = getReportingFrameworkContext();
        execute(jobConfig)
        closeSparkSession()
    }
    
    private def execute(config: JobConfig)(implicit sparkSession: SparkSession, fc: FrameworkContext) = {
    
        val resultDf = generateExternalIdReport();
        JobLogger.end("ExternalIdReportJob completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
        generateSelfUserDeclaredZip(resultDf, config)
        JobLogger.end("ExternalIdReportJob zip completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name)))
    }
    
    // $COVERAGE-ON$ Enabling scoverage for other methods
    def generateExternalIdReport() (implicit sparkSession: SparkSession, fc: FrameworkContext) = {
        import sparkSession.implicits._
        val DECLARED_EMAIL: String = "declared-email"
        val DECLARED_PHONE: String = "declared-phone"
        val userSelfDeclaredEncoder = Encoders.product[UserSelfDeclared].schema
        //loading user_declarations table details based on declared values and location details and appending org-external-id if present
        val userSelfDeclaredDataDF = loadData(sparkSession, Map("table" -> "user_declarations", "keyspace" -> sunbirdKeyspace), Some(userSelfDeclaredEncoder))
        val userConsentDataDF = loadData(sparkSession, Map("table" -> "user_consent", "keyspace" -> sunbirdKeyspace))
        val activeConsentDF = userConsentDataDF.where(col("status") === "ACTIVE" && lower(col("object_type")) ===  "organisation")
        val activeSelfDeclaredDF = userSelfDeclaredDataDF.join(activeConsentDF, userSelfDeclaredDataDF.col("userid") === activeConsentDF.col("user_id") && userSelfDeclaredDataDF.col("orgid") === activeConsentDF.col("consumer_id"), "left_semi").
           select(userSelfDeclaredDataDF.col("*"))
        val userSelfDeclaredUserInfoDataDF = activeSelfDeclaredDF.select(col("*"), col("userinfo").getItem(DECLARED_EMAIL).as(DECLARED_EMAIL), col("userinfo").getItem(DECLARED_PHONE).as(DECLARED_PHONE),
            col("userinfo").getItem("declared-school-name").as("declared-school-name"), col("userinfo").getItem("declared-school-udise-code").as("declared-school-udise-code"),col("userinfo").getItem("declared-ext-id").as("declared-ext-id")).drop("userinfo");
        val locationDF = locationData()
        //to-do later check if externalid is necessary not-null check is necessary
        val orgExternalIdDf = loadOrganisationData().select("externalid","channel", "id","orgName").filter(col("channel").isNotNull)
        val userSelfDeclaredExtIdDF = userSelfDeclaredUserInfoDataDF.join(orgExternalIdDf, userSelfDeclaredUserInfoDataDF.col("orgid") === orgExternalIdDf.col("id"), "leftouter").
            select(userSelfDeclaredUserInfoDataDF.col("*"), orgExternalIdDf.col("*"))
        
        //decrypting email and phone values
        val userDecrpytedDataDF = decryptPhoneEmailInDF(userSelfDeclaredExtIdDF, DECLARED_EMAIL, DECLARED_PHONE)
        //appending decrypted values to the user-external-identifier dataframe
        val userExternalDecryptData  = userSelfDeclaredExtIdDF.join(userDecrpytedDataDF, userSelfDeclaredExtIdDF.col("userid") === userDecrpytedDataDF.col("userid"), "left_outer").
            select(userSelfDeclaredExtIdDF.col("*"), userDecrpytedDataDF.col("decrypted-email"), userDecrpytedDataDF.col("decrypted-phone"))
    
        //loading user data with location-details based on the user's from the user-external-identifier table
        val userDf = loadData(sparkSession, Map("table" -> "user", "keyspace" -> sunbirdKeyspace), None).
            select(col(  "userid"),
                concat_ws(" ", col("firstname"), col("lastname")).as("Name"),
                col("email").as("profileemail"), col("phone").as("profilephone"), col("rootorgid"), col("profileusertypes"), col("profilelocation"))
        val userWithProfileDF = appendUserProfileTypeWithLocation(userDf);
        
        val commonUserDf = userWithProfileDF.join(userExternalDecryptData, userWithProfileDF.col("userid") === userExternalDecryptData.col("userid"), "inner").
            select(userWithProfileDF.col("*"))
        val userDenormDF = commonUserDf.withColumn("exploded_location", explode_outer(col("locationids")))
            .join(locationDF, col("exploded_location") === locationDF.col("locid") && (locationDF.col("loctype") === "cluster" || locationDF.col("loctype") === "block" || locationDF.col("loctype") === "district" || locationDF.col("loctype") === "state"), "left_outer")
        val userDenormLocationDF = userDenormDF.groupBy("userid", "Name", "usertype", "usersubtype", "profileemail", "profilephone", "rootorgid").
            pivot("loctype").agg(first("locname").as("locname"))
        
        val decryptedUserProfileDF = decryptPhoneEmailInDF(userDenormLocationDF, "profileemail", "profilephone")
        val denormLocationUserDecryptData  = userDenormLocationDF.join(decryptedUserProfileDF, userDenormLocationDF.col("userid") === decryptedUserProfileDF.col("userId"), "left_outer").
            select(userDenormLocationDF.col("*"), decryptedUserProfileDF.col("decrypted-email"), decryptedUserProfileDF.col("decrypted-phone"))
        val finalUserDf = denormLocationUserDecryptData.join(orgExternalIdDf, denormLocationUserDecryptData.col("rootorgid") === orgExternalIdDf.col("id"), "left_outer").
            select(denormLocationUserDecryptData.col("*"), orgExternalIdDf.col("orgName").as("userroororg"))
        saveUserSelfDeclaredExternalInfo(userExternalDecryptData, finalUserDf)
    }
    
    def decryptPhoneEmailInDF(userDF: DataFrame, email: String, phone: String)(implicit sparkSession: SparkSession, fc: FrameworkContext) : DataFrame = {
        val profileEmailMap = userDF.rdd.map(r => (r.getAs[String]("userid"), r.getAs[String](email))).collectAsMap()
        val profilePhoneMap = userDF.rdd.map(r => (r.getAs[String]("userid"), r.getAs[String](phone))).collectAsMap()
        val decryptedUserProfileDF = decryptDF(profileEmailMap, profilePhoneMap);
        decryptedUserProfileDF
    }
    
    def appendUserProfileTypeWithLocation(userDf: DataFrame) : DataFrame = {
        val userProfileDf = userDf.withColumn("locationids", locationIdList(col("profilelocation"))).
            withColumn("usertype", addUserType(col("profileusertypes"), lit("type"))).
            withColumn("usersubtype", addUserType(col("profileusertypes"), lit("subType")))
        userProfileDf
    }
    
    def generateSelfUserDeclaredZip(blockData: DataFrame, jobConfig: JobConfig): Unit = {
        JobLogger.log(s"generateSelfUserDeclaredZip:: Self-Declared user level zip generation::starts", None, INFO)
        val params = jobConfig.modelParams.getOrElse(Map())
        val virtualEnvDirectory = params.getOrElse("adhoc_scripts_virtualenv_dir", "/mount/venv")
        val scriptOutputDirectory = params.getOrElse("adhoc_scripts_output_dir", "/mount/portal_data")
        
        val channels = blockData.select(col("provider")).distinct().collect.map(_.getString(0)).mkString(",")
        val userDeclaredDetailReportCommand = Seq("bash", "-c",
            s"source $virtualEnvDirectory/bin/activate; " +
                s"dataproducts user_declared_detail --data_store_location='$scriptOutputDirectory' --states='$channels' --is_private")
        JobLogger.log(s"User self-declared detail persona zip report command:: $userDeclaredDetailReportCommand", None, INFO)
        val userDeclaredDetailReportExitCode = ScriptDispatcher.dispatch(userDeclaredDetailReportCommand)
        
      //$COVERAGE-OFF$ Disabling scoverage for if condition
        if (userDeclaredDetailReportExitCode == 0) {
            JobLogger.log(s"Self-Declared user level zip generation::Success", None, INFO)
        }  // $COVERAGE-ON$ Enabling scoverage for else condition
        else {
            JobLogger.log(s"Self-Declared user level zip generation failed with exit code $userDeclaredDetailReportExitCode", None, ERROR)
            throw new Exception(s"Self-Declared user level zip generation failed with exit code $userDeclaredDetailReportExitCode")
        }
    }
    
    private def decryptDF(emailMap: collection.Map[String, String], phoneMap: collection.Map[String, String]) (implicit sparkSession: SparkSession, fc: FrameworkContext) : DataFrame = {
        import sparkSession.implicits._
        //check declared-email and declared-phone position in the RDD
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
        var userDenormLocationDFWithCluster : DataFrame = null;
        if(!userDenormLocationDF.columns.contains("cluster")) {
            if(!userDenormLocationDF.columns.contains("block")) {
                userDenormLocationDFWithCluster = userDenormLocationDF.withColumn("cluster", lit("").cast("string")).withColumn("block", lit("").cast("string"))
            } else {
                userDenormLocationDFWithCluster = userDenormLocationDF.withColumn("cluster", lit("").cast("string"))
            }
        } else {
            userDenormLocationDFWithCluster = userDenormLocationDF
        }
        val resultDf = userExternalDecryptData.join(userDenormLocationDFWithCluster, userExternalDecryptData.col("userid") === userDenormLocationDFWithCluster.col("userid"), "left_outer").
            
            select(col("Name"),
                userExternalDecryptData.col("userid").as("Diksha UUID"),
                when(userDenormLocationDFWithCluster.col("state").isNotNull, userDenormLocationDFWithCluster.col("state")).otherwise(lit("")).as("State"),
                when(userDenormLocationDFWithCluster.col("district").isNotNull, userDenormLocationDFWithCluster.col("district")).otherwise(lit("")).as("District"),
                when(userDenormLocationDFWithCluster.col("block").isNotNull, userDenormLocationDFWithCluster.col("block")).otherwise(lit("")).as("Block"),
                when(userDenormLocationDFWithCluster.col("cluster").isNotNull, userDenormLocationDFWithCluster.col("cluster")).otherwise(lit("")).as("Cluster"),
                col("declared-school-name"). as("School Name"),
                col("declared-school-udise-code").as("School UDISE ID"),
                col("declared-ext-id").as("State provided ext. ID"),
                userDenormLocationDFWithCluster.col("decrypted-email").as("Profile Email"),
                userDenormLocationDFWithCluster.col("decrypted-phone").as("Profile Phone number"),
                userExternalDecryptData.col("decrypted-phone").as("Org Phone"),
                userExternalDecryptData.col("decrypted-email").as("Org Email ID"),
                col("usertype").as("User Type"),
                col("usersubtype").as("User-Sub Type"),
                col("userroororg").as("Root Org of user"),
                col("channel").as("provider"))
        resultDf.saveToBlobStore(storageConfig, "csv", "declared_user_detail", Option(Map("header" -> "true")), Option(Seq("provider")))
        resultDf
    }
    
    def locationIdListFunction(location: String): List[String] = {
        var locations = new ListBuffer[String]()
        try {
            if (location != null && !location.isEmpty) {
                val profileLocation = JSONUtils.deserialize[List[Map[String, String]]](location)
                val stateIdList = profileLocation.filter(f => f.getOrElse("type", "").equalsIgnoreCase("state")).map(f => f.getOrElse("id", ""))
                if (!stateIdList.isEmpty) locations += stateIdList.head
                val districtIdList = profileLocation.filter(f => f.getOrElse("type", "").equalsIgnoreCase("district")).map(f => f.getOrElse("id", ""))
                if (!districtIdList.isEmpty) locations += districtIdList.head
                val blockIdList = profileLocation.filter(f => f.getOrElse("type", "").equalsIgnoreCase("block")).map(f => f.getOrElse("id", ""))
                if (!blockIdList.isEmpty) locations += blockIdList.head
                val clusterIdList = profileLocation.filter(f => f.getOrElse("type", "").equalsIgnoreCase("cluster")).map(f => f.getOrElse("id", ""))
                if (!clusterIdList.isEmpty) locations += clusterIdList.head
                locations.toList
            } else locations.toList
        }catch {
            case ex: Exception =>
                JobLogger.log(s"StateAdminReportJob: error in locationIdListFunction = ${ex.getMessage}", None, ERROR)
                locations.toList
        }
    }
    
    val locationIdList = udf[List[String], String](locationIdListFunction)
    
    def parseProfileTypeFunction(profileUserTypesStr: String, typeKey: String) : String = {
        if(profileUserTypesStr != null && profileUserTypesStr.nonEmpty) {
            val profileUserType = JSONUtils.deserialize[List[Map[String, String]]](profileUserTypesStr)
            profileUserType.foldLeft(List[String]())((resultType, userType) => {
                val typeVal:String = userType.getOrElse(typeKey, "")

                if (typeVal != null && typeVal.nonEmpty && !resultType.contains(typeVal)) resultType :+ typeVal else resultType
            }).mkString(",")
        } else ""
    }

    val addUserType = udf[String, String, String](parseProfileTypeFunction)

}
