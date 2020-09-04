package org.sunbird.learner.jobs

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{ SparkSession }
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.{array_distinct, flatten}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.Column

/*
*   User: Schema for User table
*   UserExternalIdentity : Schema for usr_external_identity table
*   UserOrg:  Schema for user_org table
*   UserLookup: Schema for user_lookup table   
*/

case class User(id: String, username: Option[String], phone: Option[String], email: Option[String],rootorgid: String)
case class UserExternalIdentity(userid: String, externalid: String, idtype: String, provider: String)
case class UserLookup(`type`: String, value: String, userid: String)





object UserLookupMigration extends Serializable {

  private val config: Config = ConfigFactory.load

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession =
      SparkSession
        .builder()
        .appName("UserLookupMigration")
        .config("spark.master", "local[*]")
        .config("spark.cassandra.connection.host",config.getString("spark.cassandra.connection.host")) 
        .config("spark.cassandra.output.batch.size.rows", config.getString("spark.cassandra.output.batch.size.rows"))
        .config("spark.cassandra.read.timeoutMS",config.getString("spark.cassandra.read.timeoutMS"))
        .getOrCreate()

     
    val res = time(migrateData());

    Console.println("Time taken to execute script", res._1);
    spark.stop();
  }

def migrateData()(implicit spark: SparkSession) {
    val userschema = Encoders.product[User].schema
    val stateUserSchema = Encoders.product[UserExternalIdentity].schema
    val userLookupschema = Encoders.product[UserLookup].schema

    // Read user, user_org and usr_external identity table intot the memory 

    val userdata = spark.read.format("org.apache.spark.sql.cassandra").schema(userschema).option("keyspace", "sunbird").option("table", "user").load();
    val stateUserExternalIdData = spark.read.format("org.apache.spark.sql.cassandra").schema(stateUserSchema).option("keyspace","sunbird").option("table","usr_external_identity").load();

    //Total records upon loading from db
    println("User Table records:"+ userdata.count() );
    println("State Users Table records:"+ stateUserExternalIdData.count());
    
    // Filter out the user records where all of email, phone, username are null.

    val filteredUserData = userdata.where(col("id").isNotNull && col("username").isNotNull || col("email").isNotNull || col("phone").isNotNull && col("rootorgid").isNotNull).persist(StorageLevel.MEMORY_ONLY);
    println("user data Count : " + filteredUserData.count());

    //Remove records where userid, externalid are null and is not a state users

    val filteredStateUserData =stateUserExternalIdData.where(col("userid").isNotNull && col("externalid").isNotNull && col("idtype").isNotNull && col("provider").isNotNull && col("idtype") === col("provider")).persist(StorageLevel.MEMORY_ONLY);
    println("State Users:" +filteredStateUserData.count());
    
    //Extract email, phone, username user records 

     val emailRecords = filteredUserData.select(col("email"),col("id"),col("rootorgid")).where(col("email").isNotNull);
     val phoneRecords = filteredUserData.select(col("phone"),col("id"),col("rootorgid")).where(col("phone").isNotNull);
     val usernameRecords = filteredUserData.select(col("username"),col("id"),col("rootorgid")).where(col("username").isNotNull);

    //Remove Duplication
    val uniqueEmailRecords = emailRecords.dropDuplicates(Seq("email"))
    val uniquePhoneRecords = phoneRecords.dropDuplicates(Seq("phone"))
    val uniqueUsernameRecords = usernameRecords.dropDuplicates(Seq("username"))

    //Join records
   
    //Update value as email@orgId, phone@orgId, username@orgId, externalid@orgId

    val stateUserLookupDF = filteredStateUserData.select(col("externalId"),col("userid"),col("provider")).withColumn("type",lit("externalid")).withColumn("value",concat(col("externalid"),lit("@"),col("provider"))).withColumn("userid",col("userid")).select(col("type"),col("value"),col("userid")).where(col("value").isNotNull);

    val emailUserLookupDF = emailRecords.withColumn("type",lit("email")).withColumn("value",concat(col("email"),lit("@"),col("rootorgid"))).withColumnRenamed("id","userid").select(col("type"),col("value"),col("userid")).where(col("value").isNotNull);

    val phoneUserLookupDF = phoneRecords.withColumn("type",lit("phone")).withColumn("value",concat(col("phone"),lit("@"),col("rootorgid"))).withColumnRenamed("id","userid").select(col("type"),col("value"),col("userid")).where(col("value").isNotNull);

    val usernameUserLookupDF = usernameRecords.withColumn("type",lit("username")).withColumn("value",concat(col("username"),lit("@"),col("rootorgid"))).withColumnRenamed("id","userid").select(col("type"),col("value"),col("userid")).where(col("value").isNotNull);


    println("Email Record Count:"+emailUserLookupDF.count())
    println("Phone Record Count:"+emailUserLookupDF.count())
    println("Username Record Count:"+usernameUserLookupDF.count())
    println("Stat User Record Count:"+stateUserLookupDF.count())

    //Save records to the user_lookup table

    stateUserLookupDF.write.format("org.apache.spark.sql.cassandra").option("keyspace", "sunbird").option("table", "user_lookup").mode(SaveMode.Append).save();
    emailUserLookupDF.write.format("org.apache.spark.sql.cassandra").option("keyspace", "sunbird").option("table", "user_lookup").mode(SaveMode.Append).save();
    phoneUserLookupDF.write.format("org.apache.spark.sql.cassandra").option("keyspace", "sunbird").option("table", "user_lookup").mode(SaveMode.Append).save();
    usernameUserLookupDF.write.format("org.apache.spark.sql.cassandra").option("keyspace", "sunbird").option("table", "user_lookup").mode(SaveMode.Append).save();

    val newTableRecords = spark.read.format("org.apache.spark.sql.cassandra").schema(userLookupschema).option("keyspace", "sunbird").option("table", "user_lookup").load().count();
    println("Total Duplicate Emails: "+(emailRecords.count() - uniqueEmailRecords.count()));
    println("Total Duplicate phone: "+(phoneRecords.count() - uniquePhoneRecords.count()))
    println("Total Duplicate username: "+(usernameRecords.count() - uniqueUsernameRecords.count()))
 
    println("user_lookup count post migration: " + newTableRecords);


  }


  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }
}
