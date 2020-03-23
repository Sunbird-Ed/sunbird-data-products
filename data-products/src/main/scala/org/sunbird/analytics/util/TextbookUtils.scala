package org.sunbird.analytics.util

import java.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework.Params
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.report.ContentResult

case class ContentInformation(id: String, ver: String, ts: String, params: Params, responseCode: String,result: TextbookResult)
case class TextbookResult(count: Int, content: List[ContentResult])


object TextbookUtils {
  def getContentDataList(tenantId: String, unirest: unirestClient)(implicit sc: SparkContext): TextbookResult = {
    implicit val sqlContext = new SQLContext(sc)
    val url = Constants.COMPOSITE_SEARCH_URL
    val request = s"""{
                     |      "request": {
                     |        "filters": {
                     |           "status": ["Live","Draft","Review","Unlisted"],
                     |          "contentType": ["Resource"],
                     |          "createdFor": "$tenantId"
                     |        },
                     |        "fields": ["channel","identifier","board","gradeLevel",
                     |          "medium","subject","status","creator","lastPublishedOn","createdFor",
                     |          "createdOn","pkgVersion","contentType",
                     |          "mimeType","resourceType", "lastSubmittedOn"
                     |        ],
                     |        "limit": 10000,
                     |        "facets": [
                     |          "status"
                     |        ]
                     |      }
                     |    }""".stripMargin
    val header = new util.HashMap[String, String]()
    header.put("Content-Type", "application/json")
    val response = unirest.post(url, request, header)
    JSONUtils.deserialize[ContentInformation](response).result
  }
}
