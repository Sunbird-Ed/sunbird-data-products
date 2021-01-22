import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{ SparkSession }
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.{array_distinct, flatten}
import org.apache.spark.sql.expressions.Window

case class UserOrganisation(userid: String, organisationid: String, addedby: Option[String], addedbyname: Option[String], approvaldate: Option[String],
                            approvedby: Option[String], hashtagid: Option[String], id: String, isapproved: Option[Boolean], isdeleted: Option[Boolean],
                            isrejected: Option[Boolean], orgjoindate: Option[String], orgleftdate: Option[String], position: Option[String],
                            roles: List[String], updatedby: Option[String], updateddate: Option[String])

object UserOrgDataMigration extends Serializable {
    def main(cassandraHost: String): Unit = {
        implicit val spark: SparkSession =
            SparkSession
                .builder()
                .appName("UserOrgDataMigration")
                .config("spark.master", "local[*]")
                .config("spark.cassandra.connection.host", cassandraHost)
                .config("spark.cassandra.output.batch.size.rows", "10000")
                .config("spark.cassandra.read.timeoutMS", "60000")
                .getOrCreate()
        val res = time(migrateData());
        Console.println("Time taken to execute script", res._1);
        spark.stop();
    }
    
    def migrateData()(implicit spark: SparkSession) {
        val schema = Encoders.product[UserOrganisation].schema
        val data = spark.read.format("org.apache.spark.sql.cassandra").schema(schema).option("keyspace", "sunbird").option("table", "user_org").load();
        val filteredData = data.where(col("userid").isNotNull && col("organisationid").isNotNull).persist(StorageLevel.MEMORY_ONLY)
        println("user_org data Count : " + filteredData.count());
        val distinctRecords = filteredData.dropDuplicates("userid", "organisationid").persist();
        val dupRecords = filteredData.except(distinctRecords).select(col("userid"),col("organisationid"),col("roles")).persist(StorageLevel.MEMORY_ONLY);
        val distinctRecordsWithRoles = distinctRecords.select(col("userid"),col("organisationid"),col("roles"));
        val distinctRoleDF = distinctRecordsWithRoles.join(dupRecords, distinctRecordsWithRoles("userid") === dupRecords("userid") && distinctRecordsWithRoles("organisationid") === dupRecords("organisationid"), "leftsemi");
        val rolesDF = dupRecords.union(distinctRoleDF).withColumn("role", explode(col("roles"))).groupBy("userid","organisationid").agg(collect_set("role").as("roles"));
        distinctRecords.write.format("org.apache.spark.sql.cassandra").option("keyspace", "sunbird").option("table", "user_organisation").mode(SaveMode.Append).save()
        rolesDF.write.format("org.apache.spark.sql.cassandra").option("keyspace", "sunbird").option("table", "user_organisation").mode(SaveMode.Append).save()
        val newTableRecords = spark.read.format("org.apache.spark.sql.cassandra").schema(schema).option("keyspace", "sunbird").option("table", "user_organisation").load().count();
        println("user_organistaion count post migration: " + newTableRecords)
    }
    def time[R](block: => R): (Long, R) = {
        val t0 = System.currentTimeMillis()
        val result = block // call-by-name
        val t1 = System.currentTimeMillis()
        ((t1 - t0), result)
    }
}