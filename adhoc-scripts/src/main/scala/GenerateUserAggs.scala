import com.datastax.spark.connector._
import org.apache.spark.SparkContext

object GenerateUserAggs extends java.io.Serializable {

    case class ActivityAgg(activity_id: String, user_id: String, context_id: String, agg: Map[String, AnyRef], agg_last_updated: Map[String, AnyRef], activity_type: String = "Course") extends java.io.Serializable

    def getKey(collectionId: String, batchId: String, userId: String): String = {
        s"$collectionId:$batchId:$userId"
    }

    def migrate(sc: SparkContext): Unit = {
        val enrolments = sc.cassandraTable("sunbird_courses", "user_enrolments").select("userid", "batchid", "courseid", "progress", "completedon")
        val existingAggs = sc.cassandraTable("sunbird_courses", "user_activity_agg").select("activity_id","user_id", "context_id")

        val existingAggKeys = existingAggs.map(row => {
            val collectionId = row.getString("activity_id")
            val userId = row.getString("user_id")
            val batchId = row.getString("context_id").replaceAll("cb:", "")
            getKey(collectionId, batchId, userId)
        }).collect()
        println("Existing agg rows: " + existingAggKeys.size)
        val filteredData = enrolments.filter(row => {
            val collectionId = row.getString("courseid")
            val userId = row.getString("userid")
            val batchId = row.getString("batchid")
            val key = getKey(collectionId, batchId, userId)
            !existingAggKeys.contains(key)
        })

        val aggs = filteredData.map(row => {
            val activityId = row.getString("courseid")
            val userId = row.getString("userid")
            val contextId = "cb:" + row.getString("batchid")
            val progress = row.getInt("progress")
            val completedOn = row.getDateTime("completedon")
            val updatedOn = if (completedOn == null) System.currentTimeMillis else completedOn.getMillis
            ActivityAgg(activityId, userId, contextId, Map("completedCount" -> progress.asInstanceOf[AnyRef]), Map("completedCount" -> updatedOn.asInstanceOf[AnyRef]))
        })
        aggs.saveToCassandra("sunbird_courses", "user_activity_agg")
        println("Total rows saved: " + aggs.count())
    }
}