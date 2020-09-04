package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, concat, concat_ws, first, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.util.{CommonUtil, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, JobContext, StorageConfig}
import org.sunbird.analytics.job.report.CourseMetricsJobV2.finalColumnMapping
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.mutable

trait BaseReportsJob {

  val sunbirdKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdKeyspace")
  val sunbirdCoursesKeyspace: String = AppConf.getConfig("course.metrics.cassandra.sunbirdCoursesKeyspace")
  val sunbirdHierarchyStore: String = AppConf.getConfig("course.metrics.cassandra.sunbirdHierarchyStore")
  val metrics: mutable.Map[String, BigInt] = mutable.Map[String, BigInt]()
  val cassandraUrl = "org.apache.spark.sql.cassandra"
  val redisUrl = "org.apache.spark.sql.redis"

  def loadData(spark: SparkSession, settings: Map[String, String], schema: Option[StructType] = None): DataFrame = {
    val dataFrameReader = spark.read.format("org.apache.spark.sql.cassandra").options(settings)
    if (schema.nonEmpty) {
      schema.map(schema => dataFrameReader.schema(schema)).getOrElse(dataFrameReader).load()
    } else {
      dataFrameReader.load()
    }
  }

  def fetchData(spark: SparkSession, settings: Map[String, String], url: String, schema: Option[StructType] = None, columnNames: Option[Seq[String]] = None): DataFrame = {
    val dataSchema = schema.getOrElse(new StructType())
    import org.apache.spark.sql.functions.col
    val selectedCols = columnNames.getOrElse(Seq("*"))
    if (dataSchema.nonEmpty) {
     spark.read.schema(dataSchema).format(url).options(settings).load().select(selectedCols.map(c => col(c)): _*)
    }
    else {
      spark.read.format(url).options(settings).load().select(selectedCols.map(c => col(c)): _*)
    }
  }


  def getReportingFrameworkContext()(implicit fc: Option[FrameworkContext]): FrameworkContext = {
    fc match {
      case Some(value) => {
        value
      }
      case None => {
        new FrameworkContext();
      }
    }
  }

  def getReportingSparkContext(config: JobConfig)(implicit sc: Option[SparkContext] = None): SparkContext = {

    val sparkContext = sc match {
      case Some(value) => {
        value
      }
      case None => {
        val sparkCassandraConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkCassandraConnectionHost")
        val sparkElasticsearchConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkElasticsearchConnectionHost")
        val sparkRedisConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkRedisConnectionHost")
        val sparkUserDbRedisIndex = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkUserDbRedisIndex")
        CommonUtil.getSparkContext(JobContext.parallelization, config.appName.getOrElse(config.model), sparkCassandraConnectionHost, sparkElasticsearchConnectionHost, sparkRedisConnectionHost, sparkUserDbRedisIndex)
      }
    }
    setReportsStorageConfiguration(sparkContext)
    sparkContext;

  }

  def openSparkSession(config: JobConfig): SparkSession = {

    val sparkCassandraConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkCassandraConnectionHost")
    val sparkElasticsearchConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkElasticsearchConnectionHost")
    val sparkRedisConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkRedisConnectionHost")
    val sparkUserDbRedisIndex = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkUserDbRedisIndex")

    val readConsistencyLevel = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val sparkSession = CommonUtil.getSparkSession(JobContext.parallelization, config.appName.getOrElse(config.model), sparkCassandraConnectionHost, sparkElasticsearchConnectionHost, Option(readConsistencyLevel), sparkRedisConnectionHost, sparkUserDbRedisIndex)
    setReportsStorageConfiguration(sparkSession.sparkContext)
    sparkSession;

  }

  def closeSparkSession()(implicit sparkSession: SparkSession) {
    sparkSession.stop();
  }

  def setReportsStorageConfiguration(sc: SparkContext) {
    val reportsStorageAccountKey = AppConf.getConfig("reports_storage_key")
    val reportsStorageAccountSecret = AppConf.getConfig("reports_storage_secret")
    if (reportsStorageAccountKey != null && !reportsStorageAccountSecret.isEmpty) {
      sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.azure.account.key." + reportsStorageAccountKey + ".blob.core.windows.net", reportsStorageAccountSecret)
      sc.hadoopConfiguration.set("fs.azure.account.keyprovider." + reportsStorageAccountKey + ".blob.core.windows.net", "org.apache.hadoop.fs.azure.SimpleKeyProvider")
    }
  }

  def getStorageConfig(container: String, key: String): org.ekstep.analytics.framework.StorageConfig = {
    val reportsStorageAccountKey = AppConf.getConfig("reports_storage_key")
    val reportsStorageAccountSecret = AppConf.getConfig("reports_storage_secret")
    val provider = AppConf.getConfig("cloud_storage_type")
    if (reportsStorageAccountKey != null && !reportsStorageAccountSecret.isEmpty) {
      org.ekstep.analytics.framework.StorageConfig(provider, container, key, Option("reports_storage_key"), Option("reports_storage_secret"));
    } else {
      org.ekstep.analytics.framework.StorageConfig(provider, container, key, Option(provider), Option(provider));
    }
  }

  def customizeDF(reportDF: DataFrame, finalColumnMapping: Map[String, String], finalColumnOrder: List[String]): DataFrame = {
    val fields = reportDF.schema.fieldNames
    val colNames = for (e <- fields) yield finalColumnMapping.getOrElse(e, e)
    val dynamicColumns = fields.toList.filter(e => !finalColumnMapping.keySet.contains(e))
    val columnWithOrder = (finalColumnOrder ::: dynamicColumns).distinct
    reportDF.toDF(colNames: _*).select(columnWithOrder.head, columnWithOrder.tail: _*)
  }


}