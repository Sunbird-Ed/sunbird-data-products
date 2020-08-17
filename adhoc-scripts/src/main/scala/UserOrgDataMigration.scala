import com.datastax.spark.connector._
import org.apache.spark.SparkContext

case class UserOrganisation(userid: String, organisationid: String, addedby: Option[String], addedbyname: Option[String], approvaldate: Option[String],
                                  approvedby: Option[String], hashtagid: Option[String], id: String, isapproved: Option[Boolean],isdeleted: Option[Boolean],
                                  isrejected: Option[Boolean], orgjoindate: Option[String], orgleftdate: Option[String], position: Option[String],
                                  roles: java.util.ArrayList[String], updatedby: Option[String], updateddate: Option[String])
/**
 * Job to migrate data from user_org to user_organisation tables
 *
 * Before running the job, set `spark.cassandra.connection.host=<cassandraIp>`
 * start spaerk shell with these settings
 * --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.0 --conf spark.cassandra.connection.host=<cassandraIp>
 * 
 */
object UserOrgDataMigration {
    def migrateuserOrgdata(sc: SparkContext) = {
        val data = sc.cassandraTable[UserOrganisation]("sunbird", "user_org")
        println("user_org data Count : " + data.count())
        val filteredData = data.filter(f => (null != f.userid && null != f.organisationid))
        filteredData.saveToCassandra("sunbird", "user_organisation")
        println("user_organistaion count post migration: " + filteredData.count())
    }
}
