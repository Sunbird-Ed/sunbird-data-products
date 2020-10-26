package org.sunbird.analytics.util

import redis.clients.jedis.Jedis


class RedisCacheUtil {

  implicit val className = "org.sunbird.analytics.util.RedisCacheUtil"

  private val redisHost = "127.0.0.1"
  private val redisPort = 6379
  private val index: Int = 12

  private def getConnection(): Jedis = {
      new Jedis(redisHost, redisPort, 30000)
  }

  def getConnection(dbIndex: Int): Jedis = {
    val jedis = getConnection()
    jedis.select(dbIndex)
    jedis
  }
}
