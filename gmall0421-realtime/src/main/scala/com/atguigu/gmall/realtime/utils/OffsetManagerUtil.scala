package com.atguigu.gmall.realtime.utils

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
 * @ClassName: OffsetUtil
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/14  13:46
 * @Version: 1.0
 */
object OffsetManagerUtil {


  def getOffSet(topic: String, groupId: String): (Map[TopicPartition, Long]) = {
    //获取Redis的客户端
    val jedis: Jedis = MyRedisUtil.getClient()
    var offsetKey = "offset:" + topic + ":" + groupId
    var map: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    //将java的map转化为scala的map
    import scala.collection.JavaConverters._
    map.asScala.map {
      case (partitionID, offset) => {
        println("读取分区" + partitionID + " ： " + offset.toLong)
        (new TopicPartition(topic, partitionID.toInt), offset.toLong)
      }
    }.toMap
  }


  def saveOffset(topic: String, group: String, offsetRanges: Array[OffsetRange]) = {
    var offsetMap: util.HashMap[String, String] = new util.HashMap[String, String]()
    var offsetKey = "offset:" + topic + ":" + group
    for (elem <- offsetRanges) {
      val partition: Int = elem.partition
      val untilOffset: Long = elem.untilOffset
      offsetMap.put(partition.toString, untilOffset.toString)
      println(" 保存分区数据： " + partition + " from: " + elem.fromOffset + " to: " + untilOffset)
    }

    if (offsetMap != null && offsetMap.size() > 0) {
      //获取Redis的客户端
      val jedis: Jedis = MyRedisUtil.getClient()
      jedis.hmset(offsetKey, offsetMap)
      jedis.close()
    }

  }


}
