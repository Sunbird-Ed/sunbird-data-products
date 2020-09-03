//package org.sunbird.analytics.job.report
//
//import org.ekstep.analytics.framework.util.JSONUtils
//import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig}
//import org.ekstep.analytics.model.{OutputConfig, QueryDateRange, ReportConfig}
//import org.sunbird.analytics.util.SparkSpec
//
//class TestTextbookProgressJob extends SparkSpec(null) {
//
//  "TextbookProgressJob" should "execute and won't throw any errors/exception" in {
//    val reportConf = ReportConfig("content_progress", "", QueryDateRange(None, None, None),List(),
//      Map("review" -> "Review", "subject" -> "Subject", "application_html" -> "Uploaded Interactive Content", "resourceType" -> "Content Type",
//        "gradeLevel" -> "Grade", "vedio_youtube" -> "YouTube Content", "draft" -> "Draft", "video_mp4" -> "Uploaded Videos", "medium" -> "Medium",
//        "creator" -> "Created By", "board" -> "Board", "live" -> "Live", "status" -> "Status", "application_ecml" -> "Created on Diksha", "application_pdf" -> "Text Content"),
//      List(OutputConfig("csv",None,List(),List(),List())))
//
//    val modelParams = Map("reportConfig" -> reportConf, "bucket" -> "test-container", "key" -> "druid-reports/", "filePath" -> "src/test/resources/")
//    val con = JobConfig(Fetcher("none",None,None,None),None,None,"org.ekstep.analytics.model.TextbookProgressModel",Option(modelParams),Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),Option(8),Option("Content Progress Metrics Model"),Option(false),None,None)
//
//    TextbookProgressJob.main(JSONUtils.serialize(con))(Option(sc))
//  }
//}
