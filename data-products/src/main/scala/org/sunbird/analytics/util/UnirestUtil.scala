package org.sunbird.analytics.util

import kong.unirest.{Unirest, UnirestException}
import org.ekstep.analytics.framework.Level.ERROR
import org.ekstep.analytics.framework.util.JobLogger

trait unirestClient {
  def post(apiUrl: String, body: String, requestHeader:  java.util.HashMap[String, String]): String
}

object UnirestUtil extends unirestClient {

  implicit val className = "org.sunbird.analytics.util.UnirestUtil"

  def post(apiUrl: String, body: String, requestHeader: java.util.HashMap[String, String]): String = {
    val request = Unirest.post(apiUrl).headers(requestHeader).body(body)
    try {
      request.asString().getBody
    } catch {
      case e: UnirestException =>
        JobLogger.log(e.getMessage, Option(Map("url" -> apiUrl)), ERROR)
        e.printStackTrace();
        null
    }
  }
}
