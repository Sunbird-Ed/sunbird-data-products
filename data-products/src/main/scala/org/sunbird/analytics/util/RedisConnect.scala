//package org.sunbird.analytics.util
//
//import java.time.Duration
//
//import org.ekstep.analytics.framework.conf.AppConf
//import org.ekstep.analytics.framework.util.{JSONUtils, JobLogger}
//import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
//
//
//class RedisCacheUtil(redisHost: Option[String], redisPort: Option[Int]) {
//
//  implicit val className = "org.sunbird.analytics.util.RedisCacheUtil"
//
////  private val redisHost = "127.0.0.1"
////  private val redisPort = 6379
//  private val index: Int = 12
//
//  private def buildPoolConfig = {
//
//    val poolConfig = new JedisPoolConfig
//    poolConfig.setMaxTotal(JSONUtils.deserialize[Int](AppConf.getConfig("redis.connection.max")))
//    poolConfig.setMaxIdle(JSONUtils.deserialize[Int](AppConf.getConfig("redis.connection.idle.max")))
//    poolConfig.setMinIdle(JSONUtils.deserialize[Int](AppConf.getConfig("redis.connection.idle.min")))
//    poolConfig.setTestWhileIdle(true)
//    poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(JSONUtils.deserialize[Long](AppConf.getConfig("redis.connection.minEvictableIdleTimeSeconds"))).toMillis)
//    poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(JSONUtils.deserialize[Long](AppConf.getConfig("redis.connection.timeBetweenEvictionRunsSeconds"))).toMillis)
//    poolConfig.setBlockWhenExhausted(true)
//    poolConfig
//  }
//
//  var jedisPool = new JedisPool(buildPoolConfig, redisHost.getOrElse("127.0.0.1"), redisPort.getOrElse(6379), 100000, false)
//
//  private def getConnection(backoffTimeInMillis: Long): Jedis = {
//    if (backoffTimeInMillis > 0) try Thread.sleep(backoffTimeInMillis)
//    catch {
//      case e: Exception => throw e
//    }
//    JobLogger.log("New redis connection:")
//
//    jedisPool.getResource
//  }
//
//  def getConnection(db: Int, backoffTimeInMillis: Long): Jedis = {
//    val jedis: Jedis = getConnection(backoffTimeInMillis)
//    jedis.select(db)
//    jedis
//  }
//
//  def getConnection(dbIndex: Int): Jedis = {
//    val jedis = getConnection(backoffTimeInMillis = 0)
//    jedis.select(dbIndex)
//    jedis
//  }
//
//  def closeConnection(): Unit = {
//    jedisPool.destroy()
//    jedisPool.close()
//  }
//
//  def isActive(): Boolean = {
//    !jedisPool.isClosed
//  }
//}

package org.sunbird.analytics.util
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

class RedisConnect(redisHost: String, redisPort: Int) extends java.io.Serializable {

  private val serialVersionUID = -396824011996012513L

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