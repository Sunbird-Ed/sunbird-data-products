package org.sunbird.analytics.exhaust

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.ekstep.analytics.framework.FrameworkContext
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.sunbird.cloud.storage.conf.AppConf

trait BaseReportsJob {

  def loadData(settings: Map[String, String], url: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    if (schema.nonEmpty) {
      spark.read.schema(schema).format(url).options(settings).load()
    } else {
      spark.read.format(url).options(settings).load()
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

  def openSparkSession(config: JobConfig): SparkSession = {

    val modelParams = config.modelParams.getOrElse(Map[String, Option[AnyRef]]());
    val sparkCassandraConnectionHost = modelParams.get("sparkCassandraConnectionHost")
    val sparkElasticsearchConnectionHost = modelParams.get("sparkElasticsearchConnectionHost")
    val sparkRedisConnectionHost = modelParams.get("sparkRedisConnectionHost")
    val sparkUserDbRedisIndex = modelParams.get("sparkUserDbRedisIndex")
    JobContext.parallelization = CommonUtil.getParallelization(config)
    val readConsistencyLevel = modelParams.getOrElse("cassandraReadConsistency", "LOCAL_QUORUM").asInstanceOf[String];
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

}