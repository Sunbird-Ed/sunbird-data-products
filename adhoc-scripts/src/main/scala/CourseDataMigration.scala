import com.datastax.spark.connector._
import org.apache.spark.SparkContext

case class UserContentConsumption(userid: String, courseid: String, batchid: String, contentid: String, completedcount: Option[Int], datetime: Option[java.util.Date], lastaccesstime: Option[String] = None, lastcompletedtime: Option[String] = None, lastupdatedtime: Option[String] = None, progress: Option[Int], status: Int, viewcount: Option[Int])
case class UserEnrolments(userid: String, courseid: Option[String], batchid: String, active: Option[Boolean], addedby: Option[String], certificates: java.util.ArrayList[AnyRef],
                          completedon: Option[java.util.Date], completionpercentage: Option[Int], contentstatus: Option[java.util.Map[String, Int]], datetime: Option[java.util.Date], enrolleddate: Option[String], lastreadcontentid : Option[String],
                          lastreadcontentstatus: Option[Int], progress: Option[Int], status: Option[Int])

/**
 * Job to migrate data from content_consumption to user_content_consumption
 * and user_courses to user_enrolments tables
 * 
 * Before running the job, set `spark.cassandra.connection.host=<cassandraIp>`
 * start spaerk shell with these settings
 * --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.0 --conf spark.cassandra.connection.host=<cassandraIp>
 * 
 */
object CourseDataMigration {

    def migrateContentConsumption(sc: SparkContext) = {
        val data = sc.cassandraTable[UserContentConsumption]("sunbird_courses", "content_consumption")
        println("content_consumption data Count : " + data.count())
        val filteredData = data.filter(f => null != f.courseid)
        filteredData.saveToCassandra("sunbird_courses", "user_content_consumption")
        println("user_content_consumption count post migration: " + filteredData.count())
    }

    def migrateUserCourses(sc: SparkContext) = {
        val data = sc.cassandraTable[UserEnrolments]("sunbird_courses", "user_courses")
        println("user_courses data Count : " + data.count())
        val filteredData = data.filter(f => f.courseid.isDefined && !f.courseid.isEmpty)
        filteredData.saveToCassandra("sunbird_courses", "user_enrolments")
        println("user_enrolments count post migration: " + filteredData.count())
    }
}
