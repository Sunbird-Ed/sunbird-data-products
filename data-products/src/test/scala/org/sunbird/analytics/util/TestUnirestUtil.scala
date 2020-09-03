//package org.sunbird.analytics.util
//
//import org.ekstep.analytics.framework.util.JSONUtils
//
//case class PostError (args: Map[String, String], data: String, headers: Map[String, String], json: Map[String, AnyRef], origin: String, url: String)
//class TestUnirestUtil extends BaseSpec {
//
//  "UnirestUtil" should "throw exception if unable to produce response in POST request" in {
//    val url = "https://httpbin.org/post?type=test";
//    val request = Map("popularity" -> 1);
//    val response  = UnirestUtil.post(url, JSONUtils.serialize(request))
//    val resError = JSONUtils.deserialize[PostError](response)
//    resError.url shouldBe("https://httpbin.org/post?type=test")
//  }
//}