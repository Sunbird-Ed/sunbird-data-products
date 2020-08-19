package org.sunbird.analytics.jobs

import com.redislabs.provider.redis._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.functions.{col, collect_set, concat_ws, explode_outer, first, lit, lower, to_json, _}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.sunbird.analytics.util.JSONUtils

case class AnonymousData(userid:String, usersignintype: String, userlogintype: String)

object UserCacheIndexer extends Serializable {

  private val config: Config = ConfigFactory.load

  def main(args: Array[String]): Unit = {

    var specificUserId: String = null
    var fromSpecificDate: String = null
    var populateAnonymousData: String = "false"
    if (!args.isEmpty) {
      specificUserId = args(0) // userid
      fromSpecificDate = args(1) // date in YYYY-MM-DD format
      populateAnonymousData = args(2) // populate anonymous data
    }
    val sunbirdKeyspace = "sunbird"

    val complexFieldTypes = Array("array", "map")
    val redisIndex = if (!populateAnonymousData.equalsIgnoreCase("true")) config.getString("redis.user.database.index") else config.getString("redis.user.input.index")

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local[*]")
        .config("spark.cassandra.connection.host", config.getString("spark.cassandra.connection.host"))
        .config("spark.redis.host", config.getString("redis.host"))
        .config("spark.redis.port", config.getString("redis.port"))
        .config("spark.redis.db", redisIndex)
        .config("spark.redis.max.pipeline.size", config.getString("redis.max.pipeline.size"))
        .config("spark.cassandra.read.timeoutMS", "300000")
        .getOrCreate()

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

    def populateUserData() {

      val userDF = filterUserData(spark.read.format("org.apache.spark.sql.cassandra").option("table", "user").option("keyspace", sunbirdKeyspace).load()
        .filter(col("userid").isNotNull))
        .withColumn("medium", explode_outer(col("framework.medium")))  // Flattening the BGMS
        .withColumn("subject", explode_outer(col("framework.subject")))
        .withColumn("board", explode_outer(col("framework.board")))
        .withColumn("grade", explode_outer(col("framework.gradeLevel")))
        .withColumn("framework_id", explode_outer(col("framework.id")))
        .drop("framework")
        .withColumnRenamed("framework_id", "framework")
        .persist(StorageLevel.MEMORY_ONLY)

      Console.println("User records count:", userDF.count());
      val res1 = time(populateToRedis(userDF)) // Insert all userData Into redis
      Console.println("Time taken to insert user records", res1._1)

      val userOrgDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "user_org").option("keyspace", sunbirdKeyspace).load().filter(lower(col("isdeleted")) === "false")
        .select(col("userid"), col("organisationid"))

      val organisationDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "organisation").option("keyspace", sunbirdKeyspace).load()
        .select(col("id"), col("orgname"), col("channel"), col("orgcode"),
          col("locationids"), col("isrootorg"))

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

      val res2 = time(populateToRedis(filteredOrgDf))
      userDF.unpersist();
      Console.println("Time taken to insert user org records", res2._1)
    }

    def denormUserData(): Unit = {

      val userDF = filterUserData(spark.read.format("org.apache.spark.sql.cassandra").option("table", "user").option("keyspace", sunbirdKeyspace).load()
        .filter(col("userid").isNotNull))
        .select("userid", "locationids", "rootorgid","channel").persist(StorageLevel.MEMORY_ONLY)

      val userOrgDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "user_org").option("keyspace", sunbirdKeyspace).load().filter(lower(col("isdeleted")) === "false")
        .select(col("userid"), col("organisationid")).persist(StorageLevel.MEMORY_ONLY)

      val organisationDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "organisation").option("keyspace", sunbirdKeyspace).load()
        .select(col("id"), col("orgname"), col("channel"), col("orgcode"), col("locationids"), col("isrootorg")).persist(StorageLevel.MEMORY_ONLY)

      val locationDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "location").option("keyspace", sunbirdKeyspace).load().persist(StorageLevel.MEMORY_ONLY)

      val externalIdentityDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "usr_external_identity").option("keyspace", sunbirdKeyspace).load()
        .select(col("provider"), col("idtype"), col("externalid"), col("userid"))

      // Get CustodianOrgID
      val custRootOrgId = getCustodianOrgId()
      Console.println("#### custRootOrgId ####", custRootOrgId)

      val custodianUserDF = generateCustodianOrgUserData(custRootOrgId, userDF, organisationDF, locationDF, externalIdentityDF)

      val filteredCustoDian = custodianUserDF.select(
        col("declared-school-name").as("schoolname"),
        col("declared-ext-id").as("externalid"),
        col("state_name").as("state"),
        col("district"), col("block"),
        col("declared-school-udise-code").as("schooludisecode"),
        col("user_channel").as("userchannel"), col("userid"), col("usersignintype"))
        
      val res1 = time(populateToRedis(filteredCustoDian.distinct()))
      Console.println("Time taken to insert custodian details", res1._1)

      val stateUserDF = generateStateOrgUserData(custRootOrgId, userDF, organisationDF, locationDF, externalIdentityDF, userOrgDF)

      val filteredStateDF = stateUserDF.select(
        col("declared-school-name").as("schoolname"),
        col("declared-ext-id").as("externalid"),
        col("state_name").as("state"),
        col("district"), col("block"),
        col("declared-school-udise-code").as("schooludisecode"),
        col("user_channel").as("userchannel"), col("userid"), col("usersignintype"))

      val res2 = time(populateToRedis(filteredStateDF.distinct()))
      Console.println("Time taken to insert state user details", res2._1)
      userDF.unpersist()

    }

    def getCustodianOrgId(): String = {
      val systemSettingDF = spark.read.format("org.apache.spark.sql.cassandra").option("table", "system_settings").option("keyspace", sunbirdKeyspace).load()
      val df = systemSettingDF.where(col("id") === "custodianOrgId" && col("field") === "custodianOrgId").select(col("value"))
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
        .select(
          userDF.col("*"),
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
        .select(
          custodianOrguserLocationDF.col("userid"),
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
        .select(
          userDF.col("*"),
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

    def populateToRedis(dataFrame: DataFrame): Unit = {
      val filteredDF = dataFrame.filter(col("userid").isNotNull)
      val schema = filteredDF.schema
      val complexFields = schema.fields.filter(field => complexFieldTypes.contains(field.dataType.typeName))

      val resultDF = complexFields.foldLeft(filteredDF)((df, field) =>
        df.withColumn(field.name, to_json(col(field.name))))

      resultDF.write
        .format("org.apache.spark.sql.redis")
        .option("host", config.getString("redis.host"))
        .option("port", config.getString("redis.port"))
        .option("dbNum", config.getString("redis.user.database.index"))
        .option("table", "user")
        .option("key.column", "userid")
        .mode(SaveMode.Append)
        .save()
    }

    if (!populateAnonymousData.equalsIgnoreCase("true")) {
      val res1 = time(populateUserData())
      val res2 = time(denormUserData())
      val totalTimeTaken = (res1._1 + res2._1).toDouble/1000
      Console.println("Time taken for individual steps:", "stage1", res1._1, "stage2", res2._1)
      Console.println("Time taken for complete script:", totalTimeTaken);
    } else {
      val res = time(populateAnonymousUserData())
      Console.println("Time taken for complete script:", res._1);
    }

    def populateAnonymousUserData(): Unit = {
      val sqlContext = new SQLContext(spark.sparkContext)
      import sqlContext.implicits._

      val anonymousDataRDD = spark.sparkContext.fromRedisKV("*")
      val filteredData = anonymousDataRDD.map{f => (f._1, JSONUtils.deserialize[Map[String, AnyRef]](f._2))}.filter(f => f._2.getOrElse("usersignintype", "").equals("Anonymous"))

      val anonymousDataDF = filteredData.map{f =>
        AnonymousData(f._1, f._2.getOrElse("usersignintype", "Anonymous").toString, f._2.getOrElse("userlogintype", "").toString)
      }.toDF()
      Console.println("Anonymous data user count: " + anonymousDataDF.count())

      val res1 = time(populateToRedis(anonymousDataDF)) // Insert all userData Into redis
      Console.println("Time taken to insert anonymous records", res1._1)
    }
  }

  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }

}
