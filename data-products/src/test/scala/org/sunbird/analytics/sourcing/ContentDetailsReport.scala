package org.sunbird.analytics.sourcing

import org.apache.spark.sql.SparkSession
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, JobContext}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers
import org.sunbird.analytics.util.{BaseSpec, SparkSpec}

class TestContentDetailsReport extends SparkSpec with Matchers with MockFactory {
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    //    super.beforeAll()
    //    spark = getSparkSession()
  }

  override def afterAll() {
    //    super.afterAll()
  }

  it should "generate report" in {
    //    implicit val sc = CommonUtil.getSparkSession(JobContext.parallelization, "config.model", Option("localhost"), Option("localhost"), Option("2"), Option("localhost"), Option(12)).sparkContext
    //    implicit val mockFc = new FrameworkContext

    sc

//    val config = """{"search": {"type": "none"},"model": "org.ekstep.analytics.sourcing.SourcingMetrics","modelParams": {"reportConfig": {"id": "textbook_report","metrics": [],"labels": {"date": "Date","identifier": "Textbook ID","name": "Textbook Name","medium": "Medium","gradeLevel": "Grade","subject": "Subject","createdOn": "Created On","lastUpdatedOn": "Last Updated On","reportDate": "Report generation date","board": "Board","grade": "Grade","chapters": "Chapter Name","totalChapters": "Total number of chapters (first level sections of ToC)","status": "Textbook Status"},"output": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}, {"type": "json","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"druidConfig": {"queryType": "groupBy","dataSource": "content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "channel","aliasName": "channel"}, {"fieldName": "identifier","aliasName": "identifier","type": "Extraction","outputType": "STRING","extractionFn": [{"type": "javascript","fn": "function(str){return str == null ? null: str.split(\".\")[0]}"}]}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "createdFor","aliasName": "createdFor"}, {"fieldName": "createdOn","aliasName": "createdOn"}, {"fieldName": "lastUpdatedOn","aliasName": "lastUpdatedOn"}, {"fieldName": "board","aliasName": "board"}, {"fieldName": "medium","aliasName": "medium"}, {"fieldName": "gradeLevel","aliasName": "gradeLevel"}, {"fieldName": "subject","aliasName": "subject"}, {"fieldName": "status","aliasName": "status"}],"filters": [{"type": "equals","dimension": "contentType","value": "TextBook"}, {"type": "in","dimension": "status","values": ["Live"]}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"store": "azure","storageContainer": "'$reportPostContainer'","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "'$reportPostContainer'","sparkCassandraConnectionHost": "'$sunbirdPlatformCassandraHost'","folderPrefix": ["slug", "reportName"]},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Textbook Report Job","deviceMapping": false}""".stripMargin
    val config = """{"search": {"type": "none"},"model": "org.ekstep.analytics.sourcing.SourcingMetrics","modelParams": {"reportConfig": {"id": "textbook_report","metrics": [],"labels": {"date": "Date","identifier": "Textbook ID","name": "Textbook Name","medium": "Medium","gradeLevel": "Grade","subject": "Subject","createdOn": "Created On","lastUpdatedOn": "Last Updated On","reportDate": "Report generation date","board": "Board","grade": "Grade","chapters": "Chapter Name","totalChapters": "Total number of chapters (first level sections of ToC)","status": "Textbook Status"},"output": [{"type": "csv","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}, {"type": "json","dims": ["identifier", "channel", "name"],"fileParameters": ["id", "dims"]}]},"contentQuery": {"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "identifier","aliasName": "identifier"}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "contentType","aliasName": "contentType"}, {"fieldName": "unitIdentifiers","aliasName": "unitIdentifiers"}, {"fieldName": "collectionId","aliasName": "collectionId"}, {"fieldName": "createdBy","aliasName": "createdBy"}, {"fieldName": "creator","aliasName": "creator"}, {"fieldName": "mimeType","aliasName": "mimeType"}],"filters": [{"type": "notequals","dimension": "contentType","value": "TextBook"}, {"type": "in","dimension": "status","values": ["Live"]}, {"type": "isnotnull","dimension": "collectionId"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"textbookQuery":{"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "programId","aliasName": "programId"}, {"fieldName": "identifier","aliasName": "identifier"}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "board","aliasName": "board"}, {"fieldName": "medium","aliasName": "medium"}, {"fieldName": "gradeLevel","aliasName": "gradeLevel"}, {"fieldName": "subject","aliasName": "subject"}, {"fieldName": "status","aliasName": "status"}, {"fieldName": "acceptedContents","aliasName": "acceptedContents"}, {"fieldName": "rejectedContents","aliasName": "rejectedContents"}],"filters": [{"type": "equals","dimension": "contentType","value": "TextBook"}, {"type": "isnotnull","dimension": "programId"}, {"type": "in","dimension": "status","values": ["Draft"]}, {"type": "equals","dimension": "channel","value": "channelId"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"store": "azure","storageContainer": "'$reportPostContainer'","format": "csv","key": "druid-reports/","filePath": "druid-reports/","container": "'$reportPostContainer'","sparkCassandraConnectionHost": "'$sunbirdPlatformCassandraHost'","folderPrefix": ["slug", "reportName"]},"output": [{"to": "console","params": {"printEvent": false}}],"parallelization": 8,"appName": "Textbook Report Job","deviceMapping": false}""".stripMargin
    ContentDetailsReport.main(config)

  }

  it should "deserialize and print" in {
    val connf = """{"search":{"type":"none"},"model":"org.ekstep.analytics.updater.UpdateContentRating","modelParams":{"tenantId":"01309282781705830427","slug":"nit123","druidQuery":{"queryType": "groupBy","dataSource": "vdn-content-model-snapshot","intervals": "1901-01-01T00:00:00+00:00/2101-01-01T00:00:00+00:00","aggregations": [{"name": "count","type": "count"}],"dimensions": [{"fieldName": "programId","aliasName": "programId"}, {"fieldName": "identifier","aliasName": "identifier"}, {"fieldName": "name","aliasName": "name"}, {"fieldName": "board","aliasName": "board"}, {"fieldName": "medium","aliasName": "medium"}, {"fieldName": "gradeLevel","aliasName": "gradeLevel"}, {"fieldName": "subject","aliasName": "subject"}, {"fieldName": "status","aliasName": "status"}, {"fieldName": "acceptedContents","aliasName": "acceptedContents"}, {"fieldName": "rejectedContents","aliasName": "rejectedContents"}],"filters": [{"type": "equals","dimension": "contentType","value": "TextBook"},{"type": "isnotnull","dimension": "programId"}, {"type": "in","dimension": "status","values": ["Draft","Live"]}, {"type": "equals","dimension": "channel","value": "channel"}],"postAggregation": [],"descending": "false","limitSpec": {"type": "default","limit": 1000000,"columns": [{"dimension": "count","direction": "descending"}]}},"brokerList":"15.2.1.7:9092","topic":"sunbirddock.mvc.processor.job.request","startDate":"2020-05-10","endDate":"2020-12-01"},"output":[{"to":"kafka","params":{"brokerList":"15.2.1.7:9092","topic":"sunbirddock.mvc.processor.job.request"}}],"parallelization":8,"appName":"Content Rating Updater","deviceMapping":false}""".stripMargin

    println(JSONUtils.deserialize[JobConfig](connf))

    val jobConf = JSONUtils.deserialize[JobConfig](connf)
    val modelParams = jobConf.modelParams.get
    println(JSONUtils.serialize(modelParams.getOrElse("druidQuery","")))
    println(jobConf.modelParams.get.getOrElse("brokerList","").asInstanceOf[String])

  }

}
