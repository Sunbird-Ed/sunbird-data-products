//package org.sunbird.analytics.job.report
//
//import org.ekstep.analytics.framework.{Dispatcher, DruidQueryModel, Fetcher, JobConfig}
//import org.ekstep.analytics.framework.util.JSONUtils
//import org.ekstep.analytics.model.{Metrics, QueryDateRange, ReportConfig}
//import org.sunbird.analytics.util.SparkSpec
//
//class TestETBMetricsJob extends SparkSpec(null) {
//
//  "ETBMetricsJob" should "execute without any errors/exceptions" in {
//
//    val reportConfig = ReportConfig("etb_metrics","",QueryDateRange(None, Option(""), Option("")),List(Metrics("", "", DruidQueryModel("", "", "", Option("")))),Map("identifier"->"TextBook Id", "name"->"Textbook Name","medium"->"Medium","gradeLevel"->"Grade","subject"->"Subject","status"->"Status"),List())
//    val esConfig = Map("request" -> Map("filters" -> Map("contentType" -> List("Textbook"), "status" -> List("Live","Review","Draft"))))
//    val modelParams = Map("esConfig" -> esConfig, "reportConfig" -> reportConfig, "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/", "slugName" -> List("tn"), "textbookIds" -> List("do_1126981011606323201176", "do_112470675618004992181"))
//    val con = JobConfig(Fetcher("none", None, None, None), None, None, "org.sunbird.analytics.model.report.ETBMetricsModel", Option(modelParams), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(8), Option("ETB Metrics Model"), Option(false), None, None)
//
//    ETBMetricsJob.main(JSONUtils.serialize(con))(Option(sc))
//  }
//
//}
