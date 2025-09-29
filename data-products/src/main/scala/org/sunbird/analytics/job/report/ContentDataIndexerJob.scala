package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig}
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.sunbird.analytics.exhaust.collection.UDFUtils


object ContentDataIndexerJob extends IJob with BaseReportsJob {

    implicit val className: String = "org.sunbird.analytics.job.report.ContentDataIndexerJob"

    // $COVERAGE-OFF$ Disabling scoverage for main and execute method
    override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None) {
        JobLogger.init(jobName)
        JobLogger.start(s"$jobName started executing", Option(Map("config" -> config, "model" -> jobName)))
        implicit val jobConfig: JobConfig = JSONUtils.deserialize[JobConfig](config)
        implicit val spark: SparkSession = openSparkSession(jobConfig)
        implicit val frameworkContext: FrameworkContext = getReportingFrameworkContext()
        try {

            // Get configurations from JobConfig
            // modelParams should have sparkElasticsearchConnectionHost, elasticsearchQueryIndex, elasticsearchQueryType, elasticsearchQueryJsonString, elasticsearchReadFields
            val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
            val index =  modelParams.getOrElse("elasticsearchQueryIndex", "compositesearch")
            val docType =  modelParams.get("elasticsearchQueryType")
            // query to fetch all 'Live' status contents of objectType 'Content'
            val query = modelParams.getOrElse("elasticsearchQueryJsonString", "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"status\":{\"query\":\"Live\"}}},{\"match\":{\"objectType\":{\"query\":\"Content\"}}}]}}}")
            val fieldsCsv = modelParams.get("elasticsearchReadFields") // Not being used currently

            val indexPath = if (docType.trim.nonEmpty) s"$index/${docType.trim}" else index

            // Fetch from Elasticsearch
            val jsonRdd = sc.esJsonRDD(indexPath, query).map(_._2)
            
            // Publish records to Kafka using Dispatcher
            val topic = modelParams.getOrElse("topic", "dev.ingest")
            val bootstrap = modelParams.getOrElse("brokerList", "")
            OutputDispatcher.dispatch(Dispatcher("kafka", Map("brokerList" -> brokerList, "topic" -> topic)), jsonRdd);
            
            jsonRdd.unpersist()

        } 
        finally {
            frameworkContext.closeContext()
            spark.close()
        }
    }
}


