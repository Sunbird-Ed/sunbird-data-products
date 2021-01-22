package org.sunbird.learner.jobs


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object SelfDeclarationToConsentMigration extends Serializable {
    private val config: Config = ConfigFactory.load
    
    def main(args: Array[String]): Unit = {
        implicit val spark: SparkSession =
            SparkSession
                .builder()
                .appName("SelfDeclarationToConsentMigration")
                .config("spark.master", "local[*]")
                .config("spark.cassandra.connection.host", config.getString("spark.cassandra.connection.host"))
                .config("spark.cassandra.output.batch.size.rows", config.getString("spark.cassandra.output.batch.size.rows"))
                .config("spark.cassandra.read.timeoutMS", config.getString("spark.cassandra.output.batch.size.rows"))
                .getOrCreate()
        val res = time(migrateSelfDeclaredToConsent());
        Console.println("Time taken to execute script", res._1);
        spark.stop();
    }
    
    def migrateSelfDeclaredToConsent()(implicit spark: SparkSession): Unit = {
        val userConsentdDF = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "sunbird").option("table", "user_consent").load().persist(StorageLevel.MEMORY_ONLY)
        val userConsentObjectTypedDF = userConsentdDF.where(col("object_type").isin("global", "Organisation")).persist(StorageLevel.MEMORY_ONLY)
        println("user_consent data Count : " + userConsentObjectTypedDF.count())
        val userDeclaredDF = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace", "sunbird").option("table", "user_declarations").load().persist(StorageLevel.MEMORY_ONLY)
        println("user_declarations data Count : " + userDeclaredDF.count())
        val userDeclaredWithOutConstentDtlDF = userDeclaredDF.join(userConsentdDF, userDeclaredDF.col("userid") === userConsentObjectTypedDF.col("user_id"), "leftanti").persist();
        println("userDeclaredDF after join latest data Count : " + userDeclaredWithOutConstentDtlDF.count())
        val savedConsentDF = convertSelfDeclaredToConsentDF(userDeclaredWithOutConstentDtlDF)
        println("user_consent data inserted Count : " + savedConsentDF.count())
    }
    
    def convertSelfDeclaredToConsentDF(userDeclaredWithOutConstentDtlDF: DataFrame): DataFrame = {
        val savedConsentDF = userDeclaredWithOutConstentDtlDF.select(col("userid").as("user_id"),col("orgid").as("consumer_id"),col("orgid").as("object_id"),
            col("createdon").as("created_on"),
            col("updatedon").as("last_updated_on")).
            withColumn("consumer_type", lit("ORGANISATION")).
            withColumn("object_type", lit("Organisation")).
            withColumn("id", concat_ws(":", lit("usr-consent"), col("user_id"),col("consumer_id"), col("consumer_id"))).
            withColumn("status", lit("ACTIVE")).
            withColumn("expiry", date_format(from_utc_timestamp(date_add(col("created_on"), 100), "Asia/Kolkata"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        savedConsentDF.write.format("org.apache.spark.sql.cassandra").option("keyspace", "sunbird").option("table", "user_consent").
            mode(SaveMode.Append).save()
        savedConsentDF
    }
    
    def time[R](block: => R): (Long, R) = {
        val t0 = System.currentTimeMillis()
        val result = block
        val t1 = System.currentTimeMillis()
        ((t1 - t0), result)
    }
}
