package org.sunbird.analytics.updater

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import org.apache.spark.SparkContext
import org.apache.spark.sql.cassandra.CassandraSparkSessionFunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.driver.BatchJobDriver.getMetricJson
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.framework.{FrameworkContext, IJob, JobConfig, JobContext}
import org.joda.time.DateTime
import scala.collection.immutable.List

object CassandraMigratorJob extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.updater.CassandraMigratorJob"
//  implicit val fc = new FrameworkContext();

  def name(): String = "CassandraMigratorJob"

  override def main(config: String)(implicit sc: Option[SparkContext] = None, fc: Option[FrameworkContext] = None): Unit = {
    val jobConfig = JSONUtils.deserialize[JobConfig](config)
    implicit val sparkContext = if (sc.isEmpty) CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model)) else sc.get
    implicit val frameworkContext: FrameworkContext = if (fc.isEmpty) new FrameworkContext() else fc.get
    val jobName = jobConfig.appName.getOrElse(name)
    JobLogger.init(jobName)
    JobLogger.start(jobName + " Started executing", Option(Map("config" -> config, "model" -> name)))
    try {
      val res = CommonUtil.time({migrateData(jobConfig)})
      // generate metric event and push it to kafka topic
      val metrics = List(Map("id" -> "total-records-migrated", "value" -> res._2.asInstanceOf[AnyRef]), Map("id" -> "time-taken-secs", "value" -> Double.box(res._1 / 1000).asInstanceOf[AnyRef]))
      val metricEvent = getMetricJson(jobName, Option(new DateTime().toString(CommonUtil.dateFormat)), "SUCCESS", metrics)
      // $COVERAGE-OFF$
      if (AppConf.getConfig("push.metrics.kafka").toBoolean)
        KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
      // $COVERAGE-ON$
      JobLogger.end(jobName + " Completed successfully!", "SUCCESS", Option(Map("config" -> config, "model" -> name, "total-records-migrated" -> res._2)))
    }  catch {
      case ex: Exception =>
        JobLogger.log(ex.getMessage, None, ERROR);
        JobLogger.end(jobName + " execution failed", "FAILED", Option(Map("model" -> jobName, "statusMsg" -> ex.getMessage)));
        // generate metric event and push it to kafka topic in case of failure
        val metricEvent = getMetricJson(jobName, Option(new DateTime().toString(CommonUtil.dateFormat)), "FAILED", List())
        // $COVERAGE-OFF$
        if (AppConf.getConfig("push.metrics.kafka").toBoolean) {
          KafkaDispatcher.dispatch(Array(metricEvent), Map("topic" -> AppConf.getConfig("metric.kafka.topic"), "brokerList" -> AppConf.getConfig("metric.kafka.broker")))
        }
      // $COVERAGE-ON$
    } finally {
      CommonUtil.closeSparkContext()
    }
  }

  def migrateData(jobConfig: JobConfig)(implicit sc: SparkContext): Long = {
    val sqlContext = new SQLContext(sc)
    val modelParams =jobConfig.modelParams.get.asInstanceOf[Map[String,String]]
    val spark = sqlContext.sparkSession
    val cassandraFormat = "org.apache.spark.sql.cassandra";
    val keyspaceName =modelParams.getOrElse("keyspace","")
    val cDataTableName = modelParams.getOrElse("cassandraDataTable","")
    val cMigrateTableName = modelParams.getOrElse("cassandraMigrateTable", "")
    val result = CommonUtil.time({
    val data = {
      spark.setCassandraConf("DataCluster", CassandraConnectorConf.
        ConnectionHostParam.option(modelParams.getOrElse("cassandraDataHost","localhost")) ++ CassandraConnectorConf.
        ConnectionPortParam.option(modelParams.getOrElse("cassandraDataPort","9042")))
      spark.setCassandraConf("MigrateCluster", CassandraConnectorConf.
        ConnectionHostParam.option(modelParams.getOrElse("cassandraMigrateHost","localhost"))  ++ CassandraConnectorConf.
        ConnectionPortParam.option(modelParams.getOrElse("cassandraMigratePort","9042")))

      spark.read.format(cassandraFormat).options(Map("table" -> cDataTableName,
        "keyspace" -> keyspaceName, "cluster" -> "DataCluster")).load()
    }
       val repartitionColumns = if (!modelParams.getOrElse("repartitionColumns", "").toString.isEmpty)
        modelParams.getOrElse("repartitionColumns", "").split(",").toSeq else Seq.empty[String]
      val repartitionDF = if (repartitionColumns.size > 0) {
        data.repartition(repartitionColumns.map(f => col(f)): _*)
      }
      else data
      (repartitionDF.count(),repartitionDF)
    })
    JobLogger.log("Time to fetch data cassandra data", Some(Map("timeTaken" -> result._1, "count" -> result._2._1)), INFO)
    val dataDf = result._2._2
    val finalResult = CommonUtil.time({
    val migratedData  =
      {
        CassandraConnector(sc.getConf.set("spark.cassandra.connection.host",
          modelParams.getOrElse("cassandraMigrateHost", "localhost").toString)
          .set("spark.cassandra.connection.port", modelParams.getOrElse("cassandraMigratePort", "9042").toString))
          .withSessionDo { session =>
            session.execute(s"""TRUNCATE TABLE $keyspaceName.$cMigrateTableName""")
          }
        dataDf.write.format(cassandraFormat).options(Map("table" -> cMigrateTableName,
          "keyspace" -> keyspaceName, "cluster" -> "MigrateCluster")).option("spark.cassandra.output.ignoreNulls", true)
          .mode("append")
          .save()
      }
      (migratedData)
    })
    JobLogger.log("Time to complete migration of cassandra table", Some(Map("timeTaken" -> finalResult._1)), INFO)
    result._2._1
  }

}
