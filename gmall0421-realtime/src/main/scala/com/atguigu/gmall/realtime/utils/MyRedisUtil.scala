package com.atguigu.gmall.realtime.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


/**
 * @ClassName: MyRedisUtil
 * @Description: TODO Redis下相关配置工具类
 * @Author: VmPerson
 * @Date: 2020/9/13  14:52
 * @Version: 1.0
 */
object MyRedisUtil {

  private var jedisPool: JedisPool = null

  def build() = {
    val properties: Properties = MyConfigUtil.load("config.properties")
    val host: String = properties.getProperty("redis.host")
    val port: String = properties.getProperty("redis.port")
    val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
    //设置最大连接数
    jedisPoolConfig.setMaxTotal(150)
    //设置最大空闲
    jedisPoolConfig.setMaxIdle(20)
    //设置最小空闲
    jedisPoolConfig.setMinIdle(20)
    //设置是否忙碌等待
    jedisPoolConfig.setMaxWaitMillis(5000)
    //每次获得连接时进行测试
    jedisPoolConfig.setTestOnBorrow(true)
    jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
  }

  def getClient(): Jedis = {
    if (jedisPool == null) build()
    jedisPool.getResource
  }


  //测试一下连接配置
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = MyRedisUtil.getClient()
    println(jedis.ping())
    jedis.close()

  }


}
