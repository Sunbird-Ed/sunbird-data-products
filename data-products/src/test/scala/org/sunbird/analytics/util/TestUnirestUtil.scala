package org.sunbird.analytics.util

import kong.unirest.UnirestException
import org.ekstep.analytics.framework.util.JSONUtils

case class PostError (args: Map[String, String], data: String, headers: Map[String, String], json: Map[String, AnyRef], origin: String, url: String)
class TestUnirestUtil extends BaseSpec {

  "UnirestUtil" should "throw exception if unable to produce response in POST request" in {
    val url = "https:/httpbin.org/post?type=test";
    val request = Map("popularity" -> 1);

    try {
      UnirestUtil.post(url, JSONUtils.serialize(request))
    } catch {
      case ex: UnirestException => Console.println(s"Invalid Request for url: ${url}. The job failed with: " + ex.getMessage)
    }
  }
}