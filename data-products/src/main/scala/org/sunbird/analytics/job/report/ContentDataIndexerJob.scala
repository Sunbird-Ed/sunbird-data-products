package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.DatasetUtil.extensions
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger, RestUtil}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, Dispatcher, OutputDispatcher}
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTimeZone
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.sunbird.analytics.exhaust.collection.UDFUtils
import org.elasticsearch.spark._


object ContentDataIndexerJob extends IJob with BaseReportsJob {

    implicit val className: String = "org.sunbird.analytics.job.report.ContentDataIndexerJob"
    val jobName = "ContentDataIndexerJob"

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
            val modelParams = jobConfig.modelParams.getOrElse(Map[String, AnyRef]())
            val index =  modelParams.getOrElse("elasticsearchQueryIndex", "compositesearch").asInstanceOf[String]
            val docType =  modelParams.getOrElse("elasticsearchQueryType", "").asInstanceOf[String]
            // query to fetch all 'Live' status contents of objectType 'Content'
            val query = modelParams.getOrElse("elasticsearchQueryJsonString", "{\"query\":{\"bool\":{\"must\":[{\"match\":{\"status\":{\"query\":\"Live\"}}},{\"match\":{\"objectType\":{\"query\":\"Content\"}}}]}}}").asInstanceOf[String]
            val fieldsCsv = modelParams.getOrElse("elasticsearchReadFields", "").asInstanceOf[String]

            val indexPath = if (docType.trim.nonEmpty) s"$index/${docType.trim}" else index

            // Fetch from Elasticsearch
            val dataset = modelParams.getOrElse("dataset", "content-snapshot-data").asInstanceOf[String]
            val jsonRdd = spark.sparkContext.esJsonRDD(indexPath, query)
                .map(_._2)
                .map { rec =>
                    val recordMap = JSONUtils.deserialize[Map[String, AnyRef]](rec)
                    JSONUtils.serialize(recordMap + ("dataset" -> dataset))
                }

            // Optionally project only requested fields (plus dataset) if fieldsCsv is provided
            val filteredJsonRdd = if (fieldsCsv.trim.isEmpty) {
                jsonRdd
            } else {
                val fieldsToKeep = fieldsCsv.split(",").map(_.trim).filter(_.nonEmpty).toSet
                jsonRdd.map { rec =>
                    val recordMap = JSONUtils.deserialize[Map[String, AnyRef]](rec)
                    val projected = recordMap.filter { case (k, _) => fieldsToKeep.contains(k) } + ("dataset" -> dataset)
                    JSONUtils.serialize(projected)
                }
            }
            
            // Publish records to Kafka using Dispatcher
            val topic = modelParams.getOrElse("topic", "dev.ingest").asInstanceOf[String]
            val brokerList = modelParams.getOrElse("brokerList", "").asInstanceOf[String]
            implicit val scForDispatcher: SparkContext = spark.sparkContext
            OutputDispatcher.dispatch(Dispatcher("kafka", Map("brokerList" -> brokerList, "topic" -> topic)), filteredJsonRdd);
            
            filteredJsonRdd.unpersist()

        } 
        finally {
            frameworkContext.closeContext()
            spark.close()
        }
    }
}


