package com.atguigu.gmall.realtime.utils

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.ListBuffer

/**
 * @ClassName: OffsetManagerMySql
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/23  19:34
 * @Version: 1.0
 */
object OffsetManagerMySql {


  //获取偏移量
  def getOffset(topic: String, group: String) = {
    //查询mysql
    val sql = " select group_id,topic,topic_offset,partition_id from offset_0421 " +
      " where topic='" + topic + "' and group_id='" + group + "'"
    val list: ListBuffer[JSONObject] = MySqlUtil.queryList(sql)

    val listMap: ListBuffer[(TopicPartition, Long)] = list.map(elem => {
      val topicPartition: TopicPartition = new TopicPartition(topic, elem.getIntValue("partition_id"))
      val offset: Long = elem.getLongValue("topic_offset")
      (topicPartition, offset)
    })
    listMap.toMap
  }


}
