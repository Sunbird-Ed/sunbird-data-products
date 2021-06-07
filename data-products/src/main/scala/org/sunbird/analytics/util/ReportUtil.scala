package org.sunbird.analytics.util

import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.util.{JobLogger, RestUtil}

object ReportUtil {

  implicit val className: String = "org.sunbird.analytics.util.ReportUtil"

  def submitIngestionTask(apiUrl: String, specPath: String): Unit = {
    val source = scala.io.Source.fromFile(specPath)
    val ingestionData = try {
      source.mkString
    } catch {
      case ex: Exception =>
        JobLogger.log(s"Exception Found While reading ingestion spec. ${ex.getMessage}", None, ERROR)
        ex.printStackTrace()
        null
    } finally source.close()
    val response = RestUtil.post[Map[String, String]](apiUrl, ingestionData, None)
    JobLogger.log(s"Ingestion Task Id: $response", None, INFO)
  }

}
