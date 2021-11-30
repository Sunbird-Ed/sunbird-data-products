package org.sunbird.analytics.util

import org.ekstep.analytics.framework.util.JSONUtils
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

class TestRedisUtil extends BaseSpec {

  var redisServer: RedisServer = _
  var jedis: Jedis = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    redisServer = new RedisServer(6341)
    redisServer.start()
    val redisConnect = new RedisConnect("localhost", 6341)
    jedis = redisConnect.getConnection
    setupRedisData(jedis)
  }

  override def afterAll() : Unit = {
    super.afterAll();
    redisServer.stop();
  }

  def setupRedisData(jedis: Jedis): Unit = {
    jedis.hmset("user:user-001", JSONUtils.deserialize[java.util.Map[String, String]]("""{"firstname": "Manju", "userid": "user-001", "state": "Karnataka", "district": "bengaluru", "userchannel": "sunbird-dev", "rootorgid": "01250894314817126443", "email": "manju@ilimi.in", "usersignintype": "Validated"};"""))
    jedis.close()
  }

  "RedisCacheUtil" should "run the redis server and insert data" in {
    val redisData = jedis.hgetAll("user:user-001")

    redisData.get("userid") should be ("user-001")
    redisData.get("firstname") should be ("Manju")
    redisData.get("lastname") should be (null)
    redisData.get("state") should be ("Karnataka")
    redisData.get("district") should be ("bengaluru")
    redisData.get("userchannel") should be ("sunbird-dev")
    redisData.get("rootorgid") should be ("01250894314817126443")
  }

}