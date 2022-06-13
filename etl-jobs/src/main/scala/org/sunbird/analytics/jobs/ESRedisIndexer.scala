package org.sunbird.analytics.jobs

import com.redislabs.provider.redis._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.sunbird.analytics.util.JSONUtils

import scala.collection.Map

object ESRedisIndexer {

    private val config: Config = ConfigFactory.load

    def main(args: Array[String]): Unit = {

        val index = config.getString("elasticsearch.query.index")
        val query = config.getString("elasticsearch.query.jsonString")

        println(s"[$index] query ===> $query")

        require(!index.isEmpty && !query.isEmpty, "require valid inputs! index name and query cannot be empty!")

        val conf = new SparkConf()
            .setAppName("SparkEStoRedisIndexer")
            .setMaster("local[*]")
            // Elasticsearch settings
            .set("es.nodes", config.getString("elasticsearch.host"))
            .set("es.port", config.getString("elasticsearch.port"))
            .set("es.scroll.size", config.getString("elasticsearch.scroll.size"))
            .set("es.query", query)
            // redis settings
            .set("spark.redis.host", config.getString("redis.host"))
            .set("spark.redis.port", config.getString("redis.port"))
            .set("spark.redis.db", config.getString("redis.es.database.index"))
            .set("spark.redis.max.pipeline.size", config.getString("redis.max.pipeline.size"))

        val sc = new SparkContext(conf)
        val keys = config.getStringList("elasticsearch.index.source.keys").toArray
        val keyDelimiter = config.getString("elasticsearch.index.source.keyDelimiter")

        // todo: log details
        def getKey(data: String): String = {
            val record = JSONUtils.deserialize[Map[String, AnyRef]](data)
            keys.map(value => record(value.asInstanceOf[String])).mkString(keyDelimiter)
        }

        sc.toRedisKV(
            sc.esJsonRDD(index).map(data => (getKey(data._2), data._2))
        )
    }
}
