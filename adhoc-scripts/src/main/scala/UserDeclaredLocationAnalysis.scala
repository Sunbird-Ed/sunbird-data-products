import java.text.NumberFormat

import scala.reflect.ManifestFactory.classType

import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.V3Event
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.spark.sql.{ SparkSession, DataFrame }
import com.datastax.spark.connector.toRDDFunctions
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.util.RestUtil
import org.apache.spark.sql.functions.{ col, lower, trim }

object UserDeclaredLocationAnalysis extends optional.Application {

  case class Params(msgid: String)
  case class LocationList(count: Int, response: List[Map[String, String]])
  case class LocationResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: LocationList)

  def main(cassandraIp: String, locationIp: String): Unit = {

    implicit val spark = CommonUtil.getSparkSession(10, "LocationPopupAnalysis", Option(cassandraIp));

    val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "device_profile", "keyspace" -> "ntpprod_device_db")).load();
    
    val states = getLocationData(locationIp).select("_1").collect().map(f => f.getString(0).toLowerCase()).distinct.toList;
    val districts = getLocationData(locationIp).select("_2").collect().map(f => f.getString(0).toLowerCase()).distinct.toList;
    val count = df
      .select("device_id", "user_declared_state", "user_declared_district")
      .where(!(col("user_declared_state").isNull || trim(col("user_declared_state")) === ""))
      .where(!(lower(col("user_declared_state")).isin(states:_*) && lower(col("user_declared_district")).isin(districts:_*))).count();

    
    Console.println("Devices where user_declared_state doesn't match master data", count);
    spark.stop();
  }

  def getLocationData(locationIp: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val locationurl = "http://" + locationIp + ":9000/v1/location/search"
    val request =
      s"""
               |{
               |  "request": {
               |    "filters": {},
               |    "limit": 10000
               |  }
               |}
               """.stripMargin
    val response = RestUtil.post[LocationResponse](locationurl, request)

    val states = response.result.response.map(f => {
      if (f.getOrElse("type", "").equalsIgnoreCase("state"))
        (f.get("id").get, f.get("name").get)
      else
        ("", "")
    }).filter(f => !f._1.isEmpty)
    val districts = response.result.response.map(f => {
      if (f.getOrElse("type", "").equalsIgnoreCase("district"))
        (f.get("parentId").get, f.get("name").get)
      else
        ("", "")
    }).filter(f => !f._1.isEmpty)
    val masterData = states.map(tup1 => districts.filter(tup2 => tup2._1 == tup1._1).map(tup2 => (tup1._2, tup2._2))).flatten.distinct
    println("master size" + masterData.size)
    spark.sparkContext.parallelize(masterData).toDF()
  }

}
