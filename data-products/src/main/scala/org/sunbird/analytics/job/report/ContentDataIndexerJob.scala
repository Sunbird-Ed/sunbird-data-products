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
    
    // protected val cassandraFormat = "org.apache.spark.sql.cassandra"

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

            // Check if should enrich with additional hierarchy data
            val includeHierarchy = modelParams.getOrElse("includeHierarchy", "false").asInstanceOf[String].toBoolean

            val contentHierarchyDBSettings = Map(
                "table" -> modelParams.getOrElse("contentHierarchyTable", "content_hierarchy").asInstanceOf[String],
                "keyspace" -> modelParams.getOrElse("contentHierarchyKeyspace", "content_hierarchy").asInstanceOf[String],
                "cluster" -> modelParams.getOrElse("contentHierarchyCluster", "LMSCluster").asInstanceOf[String]
            )
            
            val finalJsonRdd = if (includeHierarchy) {
                // Flatten hierarchies to create individual JSON events for each nested/child object
                JobLogger.log("Flattening hierarchies to create individual JSON events", None, INFO)
                val hierarchyRdd = flattenAllHierarchies(spark, frameworkContext, spark.sparkContext, contentHierarchyDBSettings)
                hierarchyRdd ++ jsonRdd
            } else {
                jsonRdd
            }

            // Optionally project only requested fields (plus dataset) if fieldsCsv is provided
            val filteredJsonRdd = if (fieldsCsv.trim.isEmpty) {
                finalJsonRdd
            } else {
                val fieldsToKeep = fieldsCsv.split(",").map(_.trim).filter(_.nonEmpty).toSet
                finalJsonRdd.map { rec =>
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
    
    /**
     * Flatten hierarchy JSON by creating individual events for each nested/child object
     * Flattens all nodes that have children, stopping only at leaf nodes (no children)
     * 
     * @param hierarchyJson The hierarchy JSON as Map
     * @return List of flattened JSON events, one for each child/nested object
     */
    def flattenHierarchy(hierarchyJson: Map[String, AnyRef]): List[Map[String, AnyRef]] = {
        try {
            var flattenedList = List[Map[String, AnyRef]]()
            
            // Get children - handle both List and Array types
            val childrenRaw = hierarchyJson.getOrElse("children", null)
            val children = childrenRaw match {
                case list: List[_] => list.asInstanceOf[List[Map[String, AnyRef]]]
                case array: Array[_] => array.toList.asInstanceOf[List[Map[String, AnyRef]]]
                case _ => List.empty[Map[String, AnyRef]]
            }
            
            // Add the current node to flattened list
            // Remove children from current node to avoid deep nesting in flattened output
            val nodeWithoutChildren = hierarchyJson - "children"
            flattenedList = flattenedList :+ nodeWithoutChildren
            
            // Recursively flatten children if they exist
            if (children != null && children.nonEmpty) {
                children.foreach { child =>
                    val childFlattened = flattenHierarchy(child)
                    flattenedList = flattenedList ++ childFlattened
                }
            }
            
            flattenedList
            
        } catch {
            case e: Exception =>
                JobLogger.log(s"Error flattening hierarchy", 
                    Some(Map("error" -> e.getMessage)), INFO)
                List.empty[Map[String, AnyRef]]
        }
    }
    
    /**
     * Flatten all hierarchies and create individual JSON events for each nested/child object
     * 
     * @param spark SparkSession
     * @param fc FrameworkContext
     * @return RDD of flattened JSON events as strings
     */
    def flattenAllHierarchies(spark: SparkSession, fc: FrameworkContext, sc: SparkContext, contentHierarchyDBSettings: Map[String, String]): org.apache.spark.rdd.RDD[String] = {
        try {
            JobLogger.log("Flattening hierarchies from Cassandra", None, INFO)
            
            // Load data from Cassandra hierarchy table
            val hierarchyDF = loadData(spark, contentHierarchyDBSettings, None)
                .select("identifier", "hierarchy")
            
            val allFlattenedEvents = hierarchyDF.rdd.flatMap { row =>
                val identifier = row.getString(0)
                val hierarchyJson = row.getString(1)
                
                // Parse the JSON hierarchy string
                val hierarchyData = if (hierarchyJson != null && hierarchyJson.nonEmpty) {
                    try {
                        JSONUtils.deserialize[Map[String, AnyRef]](hierarchyJson)
                    } catch {
                        case e: Exception =>
                            JobLogger.log(s"Error parsing hierarchy for identifier: $identifier", 
                                Some(Map("error" -> e.getMessage)), INFO)
                            Map.empty[String, AnyRef]
                    }
                } else {
                    Map.empty[String, AnyRef]
                }
                
                // Flatten the hierarchy to create individual events for each nested/child object
                val flattened = if (hierarchyData.nonEmpty) {
                    flattenHierarchy(hierarchyData)
                } else {
                    List.empty[Map[String, AnyRef]]
                }
                
                // Convert each flattened object to JSON string
                flattened.map { event =>
                    JSONUtils.serialize(event)
                }
            }
            
            val count = allFlattenedEvents.count()
            JobLogger.log("Hierarchy flattening completed successfully", 
                Some(Map("total_events" -> count)), INFO)
            
            allFlattenedEvents
            
        } catch {
            case e: Exception =>
                JobLogger.log("Error flattening hierarchies from Cassandra", 
                    Some(Map("error" -> e.getMessage)), INFO)
                sc.emptyRDD[String]
        }
    }

}


