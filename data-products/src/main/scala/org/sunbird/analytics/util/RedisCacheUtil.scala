package org.sunbird.analytics.util

import redis.clients.jedis.Jedis


class RedisCacheUtil {

  implicit val className = "org.sunbird.analytics.util.RedisCacheUtil"

  private val redisHost = "127.0.0.1"
  private val redisPort = 6379.asInstanceOf[Int]
  private val index: Int = 12

//  private def buildPoolConfig = {
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
//  protected var jedisPool: JedisPool = new JedisPool(buildPoolConfig, redisHost, redisPort)
//
//  def getConnection(database: Int): Jedis = {
//    val conn = jedisPool.getResource
//    conn.select(database)
//    conn
//  }
//
//  def getConnection: Jedis = try {
//    val jedis = jedisPool.getResource
//    if (index > 0) jedis.select(index)
//    jedis
//  } catch {
//    case e: Exception => throw e
//  }
//
//  /**
//    * This Method takes a connection object and put it back to pool.
//    *
////    * @param jedis
//    */
////  protected def returnConnection(jedis: Jedis): Unit = {
////    try if (null != jedis) jedisPool.returnResource(jedis)
////    catch {
////      case e: Exception => throw e
////    }
////  }
//
//
//  def resetConnection(): Unit = {
//    jedisPool.close()
//    jedisPool = new JedisPool(buildPoolConfig, redisHost, redisPort)
//  }
//
//  def closePool() = {
//    jedisPool.close()
//  }
//
//  def checkConnection = {
//    try {
//      val conn = getConnection(2)
//      conn.close()
//      true;
//    } catch {
//      case ex: Exception => false
//    }
//  }
//
//  /**
//    * This method store string data into cache for given Key
//    *
//    * @param key
//    * @param data
//    * @param ttl
//    */
//  def set(key: String, data: String, ttl: Int = 0): Unit = {
//    val jedis = getConnection
//    try {
//      jedis.del(key)
//      jedis.set(key, data)
//      if (ttl > 0) jedis.expire(key, ttl)
//    } catch {
//      case e: Exception =>
//        JobLogger.log("Exception Occurred While Saving String Data to Redis Cache for Key : " + key + "| Exception is:"+ e, None, ERROR)
//        throw e
//    }
////    finally returnConnection(jedis)
//  }
//
//  def hmset(key: String, data: java.util.Map[String, String], ttl: Int = 0): Unit = {
//    val jedis = getConnection
//    try {
//      jedis.del(key)
//      jedis.hmset(key, data)
//      println("jedis in set: " + jedis.info())
//      if (ttl > 0) jedis.expire(key, ttl)
//    } catch {
//      case e: Exception =>
//        JobLogger.log("Exception Occurred While Saving String Data to Redis Cache for Key : " + key + "| Exception is:"+ e, None, ERROR)
//        throw e
//    }
////    finally returnConnection(jedis)
//  }
//
//  /**
//    * This method read string data from cache for a given key
//    *
//    * @param key
//    * @param ttl
//    * @param handler
//    * @return
//    */
//  def get(key: String, handler: (String) => String = defaultStringHandler, ttl: Int = 0): String = {
//    val jedis = getConnection
//    try {
//      var data = jedis.get(key)
//      if (null != handler && (null == data || data.isEmpty)) {
//        data = handler(key)
//        if (null != data && !data.isEmpty)
//          set(key, data, ttl)
//      }
//      data
//    }
//    catch {
//      case e: Exception =>
//        JobLogger.log("Exception Occurred While Fetching String Data from Redis Cache for Key : " + key + "| Exception is: "+ e, None, ERROR)
//        throw e
//    }
////    finally returnConnection(jedis)
//  }
//
//  def hgetall(key: String, handler: (String) => java.util.Map[String, String] = defaultMapHandler, ttl: Int = 0): java.util.Map[String, String] = {
//  val jedis = getConnection
//    println("jedis in get: " + jedis.info())
//    try {
//      var data = jedis.hgetAll(key)
//      if (null != handler && (null == data || data.isEmpty)) {
//        data = handler(key)
//        if (null != data && !data.isEmpty)
//          hmset(key, data, ttl)
//      }
//      data
//    } catch {
//      case e: Exception =>
//        JobLogger.log("Exception Occurred While Fetching String Data from Redis Cache for Key : " + key + "| Exception is: "+ e, None, ERROR)
//        throw e
//    }
//
//  }
//
//  /**
//    * This method delete data from cache for given key/keys
//    *
//    * @param keys
//    */
//  def delete(keys: String*): Unit = {
//    val jedis = getConnection
//    try jedis.del(keys.map(_.asInstanceOf[String]): _*)
//    catch {
//      case e: Exception =>
//        JobLogger.log("Exception Occurred While Deleting Records From Redis Cache for Identifiers : " + keys.toArray + " | Exception is : "+ e, None, ERROR)
//        throw e
//    }
////    finally returnConnection(jedis)
//  }
//
//
//  private def defaultStringHandler(objKey: String): String = {
//    //Default Implementation Can Be Provided Here
//    ""
//  }
//
//  private def defaultMapHandler(objKey: String): java.util.Map[String, String] = {
//    val map = new java.util.HashMap[String, String]()
//    map
//  }

  private def getConnection(): Jedis = {
      new Jedis(redisHost, redisPort, 30000)
  }

  def getConnection(dbIndex: Int): Jedis = {
    val jedis = getConnection()
    jedis.select(dbIndex)
    jedis
  }
}
