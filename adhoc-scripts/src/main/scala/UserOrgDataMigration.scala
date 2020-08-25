import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{ SparkSession }
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
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
    val data = spark.read.format("org.apache.spark.sql.cassandra").schema(schema).option("keyspace", "sunbird").option("table", "user_org").load().persist(StorageLevel.MEMORY_ONLY)
    println("user_org data Count : " + data.count())
    val filteredData = data.where(col("userid").isNotNull && col("organisationid").isNotNull)
    filteredData.write.format("org.apache.spark.sql.cassandra").option("keyspace", "sunbird").option("table", "user_organisation").mode(SaveMode.Append).save()
    println("user_organistaion count post migration: " + filteredData.count())
  }
  def time[R](block: => R): (Long, R) = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    ((t1 - t0), result)
  }
}