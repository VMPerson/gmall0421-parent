package com.atguigu.gmall.realtime.utils

import java.util.Properties

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * @ClassName: MyKafkaUtil
 * @Description: TODO 读取kafka配置文件工具类
 * @Author: VmPerson
 * @Date: 2020/9/13  14:52
 * @Version: 1.0
 */
object MyKafkaUtil {


  //读取kafka的集群节点相关信息
  private val kafkaProperties: Properties = MyConfigUtil.load("config.properties")
  val brokerList: String = kafkaProperties.getProperty("kafka.broker.list")
  //设置kafka的配置信息
  private val kafkaParams = collection.mutable.Map(
    "bootstrap.servers" -> brokerList,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "gmall0421_group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  //创建DStream接收输入的数据
  def collectKafkaStream(topic: String, ssc: StreamingContext) = {
    //使用spark-streaming-kafka-0-10_2.12 Direct模式0-10版本 receive模式不适合，最主要原因是，单独一个节点接收数据，
    // 交给其他节点处理，速率不好控制，容易造成资源浪费或者OOM,被压模式是针对收集与处理在一台几点可以使用被压模式，所以receive模式，背压机制也解决不了
    var dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      //这个是分配策略 ，（有优先选择和spark的Executor节点相同的broker, 大多数情况下我们选择一致性分配策略，达到分配均衡的目的）
      LocationStrategies.PreferConsistent,
      //消息读取机制（可以选择读取指定topic的数据，可以选择读取指定topic指定分区的数据）
      ConsumerStrategies.Subscribe[String, String](
        Array(topic),
        kafkaParams
      )
    )
    dStream
  }

  //重载方法，指定消费者组
  def collectKafkaStream(topic: String, ssc: StreamingContext, groupId: String) = {
    kafkaParams("group.id") = groupId
    var dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        Array(topic),
        kafkaParams
      )
    )
    dStream
  }

  //重载方法，指定消费者组，指定偏移量（后面做精准一次性消费的时候使用）
  def collectKafkaStream(topic: String, ssc: StreamingContext, groupId: String, offsets: Map[TopicPartition, Long]) = {
    kafkaParams("group.id") = groupId
    var dStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        Array(topic),
        kafkaParams,
        offsets
      )
    )
    dStream
  }


}
