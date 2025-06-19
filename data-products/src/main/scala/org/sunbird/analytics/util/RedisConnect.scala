package org.sunbird.analytics.util

import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.{Jedis, ScanParams, ScanResult}

class RedisConnect(redisHost: String, redisPort: Int) extends java.io.Serializable {


  private val logger = LoggerFactory.getLogger(classOf[RedisConnect])


  private def getConnection(backoffTimeInMillis: Long): Jedis = {
    val defaultTimeOut = 10000
    if (backoffTimeInMillis > 0) try Thread.sleep(backoffTimeInMillis)
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
    logger.info("Obtaining new Redis connection...")
    new Jedis(redisHost, redisPort, defaultTimeOut)
  }


  def getConnection(db: Int, backoffTimeInMillis: Long): Jedis = {
    val jedis: Jedis = getConnection(backoffTimeInMillis)
    jedis.select(db)
    jedis
  }

  def getConnection(db: Int): Jedis = {
    val jedis = getConnection(db, backoffTimeInMillis = 0)
    jedis.select(db)
    jedis
  }

  def getConnection: Jedis = getConnection(db = 0)
}

object RedisSafeSearch {
  def searchLeafNodes(pattern: String, maxIterations: Int = 10000, jedis: Jedis): List[(String, String)] = {
    val scanParams = new ScanParams().`match`(pattern).count(100)
    var cursor = "0"
    val results = scala.collection.mutable.ListBuffer.empty[(String, String)]
    var iterations = 0
    try {
      do {
        val scanResult: ScanResult[String] = jedis.scan(cursor, scanParams)
        cursor = scanResult.getCursor
        scanResult.getResult.forEach { key =>
          val value = jedis.get(key)
          results += ((key, value))
        }
        iterations += 1
        if (iterations >= maxIterations) {
          println(s"Max Redis scan iterations ($maxIterations) reached, aborting scan to avoid infinite loop.")
          return results.toList
        }
      } while (cursor != "0")
    } catch {
      case ex: Exception =>
        println(s"Error scanning Redis: ${ex.getMessage}")
    }
    results.toList
  }
}