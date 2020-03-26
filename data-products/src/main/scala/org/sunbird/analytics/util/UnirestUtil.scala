package org.sunbird.analytics.util

import kong.unirest.{Unirest, UnirestException}
import org.ekstep.analytics.framework.Level.ERROR
import org.ekstep.analytics.framework.util.JobLogger

trait UnirestClient {
  def post(apiUrl: String, body: String, requestHeader:  Option[java.util.HashMap[String, String]] = None): String
}

object UnirestUtil extends UnirestClient {

  implicit val className = "org.sunbird.analytics.util.UnirestUtil"

  def post(apiUrl: String, body: String, requestHeader: Option[java.util.HashMap[String, String]] = None): String = {
    val request = Unirest.post(apiUrl).headers(requestHeader.getOrElse(new java.util.HashMap())).body(body)
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