package org.sunbird.analytics.util

import java.time.Duration

import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


class RedisCacheUtil(redisHost: String, redisPort: Int) {

  implicit val className = "org.sunbird.analytics.util.RedisCacheUtil"

  //  private val redisHost = "127.0.0.1"
  //  private val redisPort = 6379
  private val index: Int = 12

  private def buildPoolConfig = {
    val poolConfig = new JedisPoolConfig
    poolConfig.setMaxTotal(JSONUtils.deserialize[Int](AppConf.getConfig("redis.connection.max")))
    poolConfig.setMaxIdle(JSONUtils.deserialize[Int](AppConf.getConfig("redis.connection.idle.max")))
    poolConfig.setMinIdle(JSONUtils.deserialize[Int](AppConf.getConfig("redis.connection.idle.min")))
    poolConfig.setTestWhileIdle(true)
    poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(JSONUtils.deserialize[Long](AppConf.getConfig("redis.connection.minEvictableIdleTimeSeconds"))).toMillis)
    poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(JSONUtils.deserialize[Long](AppConf.getConfig("redis.connection.timeBetweenEvictionRunsSeconds"))).toMillis)
    poolConfig.setBlockWhenExhausted(true)
    poolConfig
  }

  var jedisPool = new JedisPool(buildPoolConfig, redisHost, redisPort)

  private def getConnection(backoffTimeInMillis: Long): Jedis = {
    if (backoffTimeInMillis > 0) try Thread.sleep(backoffTimeInMillis)
    catch {
      case e: Exception => throw e
    }
    JobLogger.log("New redis connection:")

    jedisPool.getResource
  }

  def getConnection(db: Int, backoffTimeInMillis: Long): Jedis = {
    val jedis: Jedis = getConnection(backoffTimeInMillis)
    jedis.select(db)
    jedis
  }

  def getConnection(dbIndex: Int): Jedis = {
    val jedis = getConnection(backoffTimeInMillis = 0)
    jedis.select(dbIndex)
    jedis
  }
}
