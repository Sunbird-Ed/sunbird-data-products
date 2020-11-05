package org.sunbird.analytics.job.report

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, Query}
import org.sunbird.analytics.util.{RedisCacheUtil, SparkSpec}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

class TestAssessmentCorrectionJob extends SparkSpec(null) {

  var redisServer: RedisServer = _
  var jedis: Jedis = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    redisServer = new RedisServer(6379)
    // redis setup
    if(!redisServer.isActive) {
      redisServer.start();
    }
    val redisConnect = new RedisCacheUtil()
    jedis = redisConnect.getConnection(0)
    setupRedisData(jedis)
  }
  override def afterAll() : Unit = {
    super.afterAll();
    redisServer.stop();
  }

  def setupRedisData(jedis: Jedis): Unit = {
    jedis.set("do_2130252187329249281308", """{"ownershipType":["createdBy"],"copyright":"preprod-sso","previewUrl":"https://preprodall.blob.core.windows.net/ntp-content-preprod/content/ecml/do_21309028634691993611691-latest","plugins":"[{\"identifier\":\"org.ekstep.stage\",\"semanticVersion\":\"1.0\"},{\"identifier\":\"org.ekstep.questionset\",\"semanticVersion\":\"1.0\"},{\"identifier\":\"org.ekstep.navigation\",\"semanticVersion\":\"1.0\"},{\"identifier\":\"org.ekstep.questionunit\",\"semanticVersion\":\"1.2\"},{\"identifier\":\"org.ekstep.questionunit.mcq\",\"semanticVersion\":\"1.3\"},{\"identifier\":\"org.ekstep.keyboard\",\"semanticVersion\":\"1.1\"},{\"identifier\":\"org.ekstep.questionunit.ftb\",\"semanticVersion\":\"1.1\"},{\"identifier\":\"org.ekstep.questionunit.mtf\",\"semanticVersion\":\"1.2\"},{\"identifier\":\"org.ekstep.questionset.quiz\",\"semanticVersion\":\"1.0\"},{\"identifier\":\"org.ekstep.iterator\",\"semanticVersion\":\"1.0\"},{\"identifier\":\"org.ekstep.summary\",\"semanticVersion\":\"1.0\"}]","subject":["English"],"channel":"01275678925675724817","downloadUrl":"https://preprodall.blob.core.windows.net/ntp-content-preprod/ecar_files/do_21309028634691993611691/course-assessment-new-1-0820_1597935799380_do_21309028634691993611691_1.0.ecar","organisation":["preprod-sso"],"language":["English"],"mimeType":"application/vnd.ekstep.ecml-archive","variants":"{\"spine\":{\"ecarUrl\":\"https://preprodall.blob.core.windows.net/ntp-content-preprod/ecar_files/do_21309028634691993611691/course-assessment-new-1-0820_1597935799443_do_21309028634691993611691_1.0_spine.ecar\",\"size\":2961.0}}","editorState":"{\"plugin\":{\"noOfExtPlugins\":12,\"extPlugins\":[{\"plugin\":\"org.ekstep.contenteditorfunctions\",\"version\":\"1.2\"},{\"plugin\":\"org.ekstep.keyboardshortcuts\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.richtext\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.iterator\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.navigation\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.reviewercomments\",\"version\":\"1.0\"},{\"plugin\":\"org.ekstep.questionunit.mtf\",\"version\":\"1.2\"},{\"plugin\":\"org.ekstep.questionunit.mcq\",\"version\":\"1.3\"},{\"plugin\":\"org.ekstep.keyboard\",\"version\":\"1.1\"},{\"plugin\":\"org.ekstep.questionunit.reorder\",\"version\":\"1.1\"},{\"plugin\":\"org.ekstep.questionunit.sequence\",\"version\":\"1.1\"},{\"plugin\":\"org.ekstep.questionunit.ftb\",\"version\":\"1.1\"}]},\"stage\":{\"noOfStages\":1,\"currentStage\":\"500ac043-1c0e-400e-b6b0-c3b36b255662\",\"selectedPluginObject\":\"a07792e8-d450-4089-8aea-2d03d1168280\"},\"sidebar\":{\"selectedMenu\":\"settings\"}}","gradeLevel":["Class 7"],"appIcon":"https://ntpproductionall.blob.core.windows.net/ntp-content-production/content/do_31265192603086028821867/artifact/..12_1544424564502.png","assets":[],"appId":"preprod.diksha.portal","contentEncoding":"gzip","artifactUrl":"https://preprodall.blob.core.windows.net/ntp-content-preprod/content/do_21309028634691993611691/artifact/1597935799290_do_21309028634691993611691.zip","lockKey":"a6573a99-eaee-4410-b957-40e614181ca0","contentType":"SelfAssess","lastUpdatedBy":"9e64cc1b-f9f0-4642-8cb3-4fb4afcb5c77","audience":["Learner"],"visibility":"Default","consumerId":"2eaff3db-cdd1-42e5-a611-bebbf906e6cf","mediaType":"content","osId":"org.ekstep.quiz.app","lastPublishedBy":"f3d5958a-6fef-4a7c-b402-ca6ab64a6ba8","version":2,"pragma":[],"license":"CC BY 4.0","prevState":"Review","size":192353,"lastPublishedOn":"2020-08-20T15:03:19.370+0000","IL_FUNC_OBJECT_TYPE":"Content","name":"Course assessment new 1 - 0820","topic":[],"status":"Live","totalQuestions":5,"code":"org.sunbird.PkzPnN","prevStatus":"Processing","description":"Enter description for Assessment","medium":["English"],"streamingUrl":"https://preprodall.blob.core.windows.net/ntp-content-preprod/content/ecml/do_21309028634691993611691-latest","idealScreenSize":"normal","createdOn":1597935345000,"copyrightYear":2020,"contentDisposition":"inline","lastUpdatedOn":1597935797000,"licenseterms":"By creating any type of content (resources, books, courses etc.) on DIKSHA, you consent to publish it under the Creative Commons License Framework. Please choose the applicable creative commons license you wish to apply to your content.","SYS_INTERNAL_LAST_UPDATED_ON":"2020-08-20T15:03:20.595+0000","dialcodeRequired":"No","createdFor":["01275678925675724817"],"creator":"K13 Content Creator","lastStatusChangedOn":1597935800000,"IL_SYS_NODE_TYPE":"DATA_NODE","os":["All"],"totalScore":5,"pkgVersion":1,"versionKey":"1597935797205","idealScreenDensity":"hdpi","framework":"mh_k-12_1","s3Key":"ecar_files/do_21309028634691993611691/course-assessment-new-1-0820_1597935799380_do_21309028634691993611691_1.0.ecar","lastSubmittedOn":"2020-08-20T15:02:36.000+0000","createdBy":"9e64cc1b-f9f0-4642-8cb3-4fb4afcb5c77","compatibilityLevel":2,"IL_UNIQUE_ID":"do_21309028634691993611691","board":"State (Maharashtra)"}""")
    jedis.close()
  }

  "AssessmentCorrectionJob" should "execute AssessmentCorrectionJob job and won't throw any error" in {

    val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/assessment-correction/test-data.log"))))), null, null , "org.sunbird.analytics.model.report.AssessmentCorrectionModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestAssessmentCorrection"), Option(true))
    AssessmentCorrectionJob.main(JSONUtils.serialize(config))(Option(sc))
  }
}
