package org.sunbird.analytics.jobs

import com.redislabs.provider.redis._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_set, concat_ws, explode_outer, first, lit, lower, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sunbird.analytics.util.JSONUtils

object UserCacheIndexer {

  private val config: Config = ConfigFactory.load

  def main(args: Array[String]): Unit = {

    var specificUserId: String = null
    var fromSpecificDate: String = null
    if (!args.isEmpty) {
      specificUserId = args(0) // userid
      fromSpecificDate = args(1) // date in YYYY-MM-DD format
    }
    val sunbirdKeyspace = "sunbird"
    val redisKeyProperty = "userid" // userid


    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local")
        .config("spark.cassandra.connection.host", config.getString("spark.cassandra.connection.host"))
        .config("spark.redis.host", config.getString("redis.host"))
        .config("spark.redis.port", config.getString("redis.port"))
        .config("spark.redis.db", config.getString("redis.user.database.index"))
        .config("spark.redis.max.pipeline.size", config.getString("redis.max.pipeline.size"))
        .config("spark.cassandra.read.timeoutMS", "300000")
        .getOrCreate()

    def filterData(param: Seq[AnyRef]): Seq[(String, String)]
    = {
      param.map {
        case (x, y) => (x.toString, if (!y.isInstanceOf[String]) {
          if (null != y) {
            JSONUtils.serialize(y.asInstanceOf[AnyRef])
          } else {
            ""
          }
        } else {
          y.asInstanceOf[String]
        })
      }
    }

    def filterUserData(userDF: DataFrame): DataFrame = {
      if (null != specificUserId && !StringUtils.equalsIgnoreCase(specificUserId, "null")) {
        println("Filtering for " + specificUserId)
        userDF.filter(col("id") === specificUserId)
      } else if (null != fromSpecificDate && !StringUtils.equalsIgnoreCase(fromSpecificDate, "null")) {
        println(s"Filtering for :$fromSpecificDate ")
        userDF.filter(col("updateddate").isNull || to_date(col("updateddate"), "yyyy-MM-dd HH:mm:ss:SSSZ").geq(lit(fromSpecificDate)))
      } else {
        println("No inputs found!! Returning user data without filter")
        userDF
      }
    }

    def denormUserData(): Unit = {

      val userDF = filterUserData(spark.read.format("org.apache.spark.sql.cassandra").option("table", "user").option("keyspace", sunbirdKeyspace).load()
        // Flattening the BGMS
        .withColumn("medium", explode_outer(col("framework.medium")))
        .withColumn("subject", explode_outer(col("framework.subject")))
        .withColumn("board", explode_outer(col("framework.board")))
        .withColumn("grade", explode_outer(col("framework.gradeLevel")))
        .withColumn("framework_id", explode_outer(col("framework.id")))
        .drop("framework"))
      val selectedUserDF = userDF.select(
        col("*"), // It populate all user data
        col("framework_id").as("framework")
      ).drop("framework_id").persist()

      populateToRedis(selectedUserDF, "USER_DF") // Insert all userData Into redis

      val userOrgDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "user_org").option("keyspace", sunbirdKeyspace).load().filter(lower(col("isdeleted")) === "false")
        .select(col("userid"), col("organisationid")).persist()

      val organisationDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "organisation").option("keyspace", sunbirdKeyspace).load()
        .select(col("id"), col("orgname"), col("channel"), col("orgcode"),
          col("locationids"), col("isrootorg")).persist()

      val locationDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "location").option("keyspace", sunbirdKeyspace).load()
      //.select(col("id"), col("name"), col("type")).persist()

      val externalIdentityDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "usr_external_identity").option("keyspace", sunbirdKeyspace).load()
        .select(col("provider"), col("idtype"), col("externalid"), col("userid")).persist()
      // Get CustodianOrgID

      val custRootOrgId = getCustodianOrgId()

      val custodianUserDF = generateCustodianOrgUserData(custRootOrgId, userDF, organisationDF, locationDF, externalIdentityDF)

      val filteredCustoDian = custodianUserDF.select(
        col("declared-school-name").as("schoolname"),
        col("declared-ext-id").as("externalid"),
        col("state_name").as("state"),
        col("district"), col("block"),
        col("declared-school-udise-code").as("schooludisecode"),
        col("user_channel").as("userchannel"), col("userid"), col("usersignintype")
      )
      populateToRedis(filteredCustoDian.distinct(), "CUSTODIAN")

      val stateUserDF = generateStateOrgUserData(custRootOrgId, userDF, organisationDF, locationDF, externalIdentityDF, userOrgDF)

      val filteredStateDF = stateUserDF.select(
        col("declared-school-name").as("schoolname"),
        col("declared-ext-id").as("externalid"),
        col("state_name").as("state"),
        col("district"), col("block"),
        col("declared-school-udise-code").as("schooludisecode"),
        col("user_channel").as("userchannel"), col("userid"), col("usersignintype"))

      populateToRedis(filteredStateDF.distinct(), "STATE")

      val userLocationResolvedDF = custodianUserDF.unionByName(stateUserDF) // UserLocation
      /**
       * Get a union of RootOrg and SubOrg information for a User
       */
      val userRootOrgDF = userDF
        .join(userOrgDF, userOrgDF.col("userid") === userDF.col("userid") && userOrgDF.col("organisationid") === userDF.col("rootorgid"))
        .select(userDF.col("userid"), col("rootorgid"), col("organisationid"), userDF.col("channel"))

      val userSubOrgDF = userDF
        .join(userOrgDF, userOrgDF.col("userid") === userDF.col("userid") && userOrgDF.col("organisationid") =!= userDF.col("rootorgid"))
        .select(userDF.col("userid"), col("rootorgid"), col("organisationid"), userDF.col("channel"))

      val rootOnlyOrgDF = userRootOrgDF.join(userSubOrgDF, Seq("userid"), "leftanti").select(userRootOrgDF.col("*"))
      val userOrgDenormDF = rootOnlyOrgDF.union(userSubOrgDF)

      /**
       * Resolve organization name for a RootOrg
       */
      val resolvedOrgNameDF = userOrgDenormDF
        .join(organisationDF, organisationDF.col("id") === userOrgDenormDF.col("rootorgid"), "left_outer")
        .groupBy("userid")
        .agg(concat_ws(",", collect_set("orgname")).as("orgname_resolved"))
      val filteredOrgDf = resolvedOrgNameDF.select(col("userid"), col("orgname_resolved").as("orgname"))

      populateToRedis(filteredOrgDf, "ORG_NAME")

      userOrgDF.unpersist()
      organisationDF.unpersist()
      locationDF.unpersist()
      externalIdentityDF.unpersist()
      userDF.unpersist()
    }


    def getCustodianOrgId(): String = {
      val systemSettingDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "system_settings").option("keyspace", sunbirdKeyspace).load()
      val df = systemSettingDF.where(col("id") === "custodianOrgId" && col("field") === "custodianOrgId")
        .select(col("value")).persist()
      df.select("value").first().getString(0)
    }

    def generateCustodianOrgUserData(custodianOrgId: String, userDF: DataFrame, organisationDF: DataFrame,
                                     locationDF: DataFrame, externalIdentityDF: DataFrame): DataFrame = {
      /**
       * Resolve the state, district and block information for CustodianOrg Users
       * CustodianOrg Users will have state, district and block (optional) information
       */

      val userExplodedLocationDF = userDF
        .withColumn("exploded_location", explode_outer(col("locationids")))
        .select(col("userid"), col("exploded_location"), col("locationids"))


      val userStateDF = userExplodedLocationDF
        .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "state")
        .select(userExplodedLocationDF.col("userid"), col("name").as("state_name"))

      val userDistrictDF = userExplodedLocationDF
        .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "district")
        .select(userExplodedLocationDF.col("userid"), col("name").as("district"))

      val userBlockDF = userExplodedLocationDF
        .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "block")
        .select(userExplodedLocationDF.col("userid"), col("name").as("block"))

      /**
       * Join with the userDF to get one record per user with district and block information
       */

      val custodianOrguserLocationDF = userDF.filter(col("rootorgid") === lit(custodianOrgId))
        .join(userStateDF, Seq("userid"), "inner")
        .join(userDistrictDF, Seq("userid"), "left")
        .join(userBlockDF, Seq("userid"), "left")
        .select(userDF.col("*"),
          col("state_name"),
          col("district"),
          col("block"))
          .withColumn("usersignintype", lit("Self-Signed-In"))
      // .drop(col("locationids"))

      val custodianUserPivotDF = custodianOrguserLocationDF
        .join(externalIdentityDF, externalIdentityDF.col("userid") === custodianOrguserLocationDF.col("userid"), "left")
        .join(organisationDF, externalIdentityDF.col("provider") === organisationDF.col("channel")
          && organisationDF.col("isrootorg").equalTo(true), "left")
        .groupBy(custodianOrguserLocationDF.col("userid"), organisationDF.col("id"))
        .pivot("idtype", Seq("declared-ext-id", "declared-school-name", "declared-school-udise-code"))
        .agg(first(col("externalid")))
        .select(custodianOrguserLocationDF.col("userid"),
          col("declared-ext-id"),
          col("declared-school-name"),
          col("declared-school-udise-code"),
          organisationDF.col("id").as("user_channel"))

      val custodianUserDF = custodianOrguserLocationDF.as("userLocDF")
        .join(custodianUserPivotDF, Seq("userid"), "left")
        .select("userLocDF.*", "declared-ext-id", "declared-school-name", "declared-school-udise-code", "user_channel")
      custodianUserDF
    }

    def generateStateOrgUserData(custRootOrgId: String, userDF: DataFrame, organisationDF: DataFrame, locationDF: DataFrame,
                                 externalIdentityDF: DataFrame, userOrgDF: DataFrame): DataFrame = {
      val stateOrgExplodedDF = organisationDF.withColumn("exploded_location", explode_outer(col("locationids")))
        .select(col("id"), col("exploded_location"))

      val orgStateDF = stateOrgExplodedDF.join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "state")
        .select(stateOrgExplodedDF.col("id"), col("name").as("state_name"))

      val orgDistrictDF = stateOrgExplodedDF
        .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "district")
        .select(stateOrgExplodedDF.col("id"), col("name").as("district"))

      val orgBlockDF = stateOrgExplodedDF
        .join(locationDF, col("exploded_location") === locationDF.col("id") && locationDF.col("type") === "block")
        .select(stateOrgExplodedDF.col("id"), col("name").as("block"))

      val stateOrgLocationDF = organisationDF
        .join(orgStateDF, Seq("id"))
        .join(orgDistrictDF, Seq("id"), "left")
        .join(orgBlockDF, Seq("id"), "left")
        .select(organisationDF.col("id").as("orgid"), col("orgname"),
          col("orgcode"), col("isrootorg"), col("state_name"), col("district"), col("block"))

      // exclude the custodian user != custRootOrgId
      // join userDf to user_orgDF and then join with OrgDF to get orgname and orgcode ( filter isrootorg = false)

      val subOrgDF = userOrgDF
        .join(stateOrgLocationDF, userOrgDF.col("organisationid") === stateOrgLocationDF.col("orgid")
          && stateOrgLocationDF.col("isrootorg").equalTo(false))
        .dropDuplicates(Seq("userid"))
        .select(col("userid"), stateOrgLocationDF.col("*"))

      val stateUserLocationResolvedDF = userDF.filter(col("rootorgid") =!= lit(custRootOrgId))
        .join(subOrgDF, Seq("userid"), "left")
        .select(userDF.col("*"),
          subOrgDF.col("orgname").as("declared-school-name"),
          subOrgDF.col("orgcode").as("declared-school-udise-code"),
          subOrgDF.col("state_name"),
          subOrgDF.col("district"),
          subOrgDF.col("block"))
        .withColumn("usersignintype", lit("Validated"))
      // .drop(col("locationids"))

      val stateUserDF = stateUserLocationResolvedDF.as("state_user")
        .join(externalIdentityDF, externalIdentityDF.col("idtype") === col("state_user.channel")
          && externalIdentityDF.col("provider") === col("state_user.channel")
          && externalIdentityDF.col("userid") === col("state_user.userid"), "left")
        .select(col("state_user.*"), externalIdentityDF.col("externalid").as("declared-ext-id"), col("rootorgid").as("user_channel"))
      stateUserDF
    }

    def populateToRedis(dataFrame: DataFrame, from: String): Unit = {
      val filteredDF = dataFrame.filter(col("userid").isNotNull)
      val fieldNames = filteredDF.schema.fieldNames
      val mappedData = filteredDF.rdd.map(row => fieldNames.map(field => field -> row.getAs(field)).toMap).collect().map(x => (x.getOrElse(redisKeyProperty, ""), x.toSeq))
      mappedData.foreach(y => {
        println(s"$from INSERTED " + y._1)
        spark.sparkContext.toRedisHASH(spark.sparkContext.parallelize(filterData(y._2)), y._1)
      })
    }

    denormUserData()
  }


}
