package org.sunbird.analytics.job.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.{FrameworkContext, JobConfig, JobContext}
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils}
import org.sunbird.cloud.storage.conf.AppConf

trait BaseReportsJob {

  def loadData(spark: SparkSession, settings: Map[String, String], schema: Option[StructType] = None): DataFrame = {
    val dataFrameReader = spark.read.format("org.apache.spark.sql.cassandra").options(settings)
    if (schema.nonEmpty) {
      schema.map(schema => dataFrameReader.schema(schema)).getOrElse(dataFrameReader).load()
    } else {
      dataFrameReader.load()
    }

  }

  // $COVERAGE-OFF$ Disabling scoverage
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
    setReportsStorageConfiguration(sparkContext, config)
    sparkContext;

  }

  def openSparkSession(config: JobConfig): SparkSession = {

    val sparkCassandraConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkCassandraConnectionHost")
    val sparkElasticsearchConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkElasticsearchConnectionHost")
    val sparkRedisConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkRedisConnectionHost")
    val sparkUserDbRedisIndex = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkUserDbRedisIndex")
    val sparkUserDbRedisPort = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).getOrElse("sparkUserDbRedisPort", "6379")
    val readConsistencyLevel = AppConf.getConfig("course.metrics.cassandra.input.consistency")
    val sparkSession = CommonUtil.getSparkSession(JobContext.parallelization, config.appName.getOrElse(config.model), sparkCassandraConnectionHost, sparkElasticsearchConnectionHost, Option(readConsistencyLevel), sparkRedisConnectionHost, sparkUserDbRedisIndex, Option(sparkUserDbRedisPort))
    setReportsStorageConfiguration(sparkSession.sparkContext, config)
    sparkSession;

  }

  def closeSparkSession()(implicit sparkSession: SparkSession) {
    sparkSession.stop();
  }

  def setReportsStorageConfiguration(sc: SparkContext, config: JobConfig) {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    val reportsStorageAccountKey = modelParams.getOrElse("storageKeyConfig", "reports_storage_key").asInstanceOf[String];
    val reportsStorageAccountSecret = modelParams.getOrElse("storageSecretConfig", "reports_storage_secret").asInstanceOf[String];
    if (reportsStorageAccountKey != null && reportsStorageAccountSecret.nonEmpty) {
      sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.azure.account.key." + AppConf.getConfig(reportsStorageAccountKey) + ".blob.core.windows.net", AppConf.getConfig(reportsStorageAccountSecret))
      sc.hadoopConfiguration.set("fs.azure.account.keyprovider." + AppConf.getConfig(reportsStorageAccountKey) + ".blob.core.windows.net", "org.apache.hadoop.fs.azure.SimpleKeyProvider")
    }
  }
  // $COVERAGE-ON$ Enabling scoverage for all other functions

  def getStorageConfig(container: String, key: String, config: JobConfig = JSONUtils.deserialize[JobConfig]("""{}""")): org.ekstep.analytics.framework.StorageConfig = {
    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]())
    val reportsStorageAccountKey = modelParams.getOrElse("storageKeyConfig", "reports_storage_key").asInstanceOf[String];
    val reportsStorageAccountSecret = modelParams.getOrElse("storageSecretConfig", "reports_storage_secret").asInstanceOf[String];
    val provider = AppConf.getConfig("cloud_storage_type")
    if (reportsStorageAccountKey != null && reportsStorageAccountSecret.nonEmpty) {
      org.ekstep.analytics.framework.StorageConfig(provider, container, key, Option(reportsStorageAccountKey), Option(reportsStorageAccountSecret))
    } else {
      org.ekstep.analytics.framework.StorageConfig(provider, container, key, Option(provider), Option(provider));
    }
  }

  def fetchData(spark: SparkSession, settings: Map[String, String], url: String, schema: StructType): DataFrame = {
    if (schema.nonEmpty) {
      spark.read.schema(schema).format(url).options(settings).load()
    }
    else {
      spark.read.format(url).options(settings).load()
    }
  }
}