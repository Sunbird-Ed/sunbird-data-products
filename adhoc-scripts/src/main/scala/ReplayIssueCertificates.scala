import java.util.UUID
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.{Dispatcher, FrameworkContext, OutputDispatcher}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}

object ReplayIssueCertificates {

    case class Event(eid: String, ets:Long, mid: String, actor: Map[String, AnyRef], context: Map[String, AnyRef], `object`: Map[String, AnyRef], edata: Map[String, AnyRef])
    val batchSize = 1

    def prepareEvent(batchId: String, courseId: String, usersList: List[List[String]], reIssue: Boolean): Seq[String] = {
        usersList.map(users => {
            val actor = Map("id" -> "Course Certificate Generator", "type" -> "System")
            val context = Map("id" -> "org.sunbird.platform", "ver" -> "1.0")
            val obj = Map("id" -> (batchId + "_" + courseId), "type" -> "CourseCertificateGeneration")
            val edata = Map("batchId" -> batchId, "courseId" -> courseId, "userIds" -> users, "action" -> "issue-certificate", "iteration" -> 1.asInstanceOf[AnyRef], "reIssue"-> reIssue.asInstanceOf[AnyRef])
            val event = Event("BE_JOB_REQUEST", System.currentTimeMillis, ("LP." + System.currentTimeMillis + "." + UUID.randomUUID), actor, context, obj, edata)
            JSONUtils.serialize(event)
        }).toSeq
    }
    def main(sc: SparkContext, batchId: String, courseId: String, env: String, kafkaBrokerList: String, reIssue: Boolean = false): Unit = {
        implicit val sparkContext = sc
        implicit val fc = new FrameworkContext()
        val data = sc.cassandraTable("sunbird_courses", "user_enrolments").select("userid", "batchid", "courseid", "status", "certificates").where("courseid = ?", courseId).where("batchid = ?", batchId).cache()
        val userIds = data.collect().filter(row => 2 == row.getInt("status")  && (null == row.getList[Map[String, AnyRef]]("certificates") || row.getList[Map[String, AnyRef]]("certificates").isEmpty)).map(row => row.getString("userid")).toList
        val usersList = userIds.grouped(batchSize).toList
        val event: RDD[String] = sc.parallelize[String](prepareEvent(batchId, courseId, usersList, reIssue))
        val config = Map("topic" -> (env +".coursebatch.certificate.request"), "brokerList" -> kafkaBrokerList)
        OutputDispatcher.dispatch(Dispatcher("kafka", config), event);
        println("Number of events pushed are: " + usersList.size)
    }

}
