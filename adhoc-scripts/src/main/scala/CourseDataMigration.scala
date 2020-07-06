import com.datastax.spark.connector._
import org.apache.spark.SparkContext

case class UserContentConsumption(userid: String, courseid: String, batchid: String, contentid: String, completedcount: Option[Int], datetime: Option[java.util.Date], lastaccesstime: Option[String] = None, lastcompletedtime: Option[String] = None, lastupdatedtime: Option[String] = None, progress: Option[Int], status: Int, viewcount: Option[Int])
case class UserEnrolments(userid: String, courseid: Option[String], batchid: String, active: Option[Boolean], addedby: Option[String], certificates: java.util.ArrayList[AnyRef],
                          completedon: Option[java.util.Date], completionpercentage: Option[Int], contentstatus: Option[java.util.Map[String, Int]], datetime: Option[java.util.Date], enrolleddate: Option[String], lastreadcontentid : Option[String],
                          lastreadcontentstatus: Option[Int], progress: Option[Int], status: Option[Int])


object CourseDataMigration {
    def printStatement(value: String) = {
        println(value)
    }

    def migrateContentConsumption(sc: SparkContext, keyspace: String) = {
        val data = sc.cassandraTable[UserContentConsumption](keyspace, "content_consumption")
        printStatement("Data Count : " + data.count())
        val filteredData = data.filter(f => null != f.courseid)
        filteredData.saveToCassandra(keyspace, "user_content_consumption")
        printStatement("Count migratd: " + filteredData.count())
    }

    def migrateUserCourses(sc: SparkContext, keyspace: String) = {
        val data = sc.cassandraTable[UserEnrolments](keyspace, "user_courses")
        printStatement("Data Count : " + data.count())
        val filteredData = data.filter(f => f.courseid.isDefined && !f.courseid.isEmpty)
        filteredData.saveToCassandra(keyspace, "user_enrolments")
        printStatement("Count migrated: " + filteredData.count())
    }

    def migrateSunbirdCourses(sc: SparkContext, host: Option[String], keyspace: Option[String], table: String = ""): Unit = {
        if(host.isDefined) {
            sc.getConf.set("spark.cassandra.connection.host", host.get)
            if(keyspace.isDefined){
                table match {
                    case "content_consumption" => migrateContentConsumption(sc, keyspace.get)
                    case "user_courses" => migrateUserCourses(sc, keyspace.get)
                    case "_" => println("Table should be one of content_consumption or user_courses")
                }
            }
        } else {
            println("Host is not defined")
        }
    }

}
