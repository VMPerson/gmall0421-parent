package com.atguigu.gmall.realtime.utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * @ClassName: MyKafkaSink
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/16  9:25
 * @Version: 1.0
 */
object MyKafkaSink {

  private val properties: Properties = MyConfigUtil.load("config.properties")
  private val brokerList: String = properties.getProperty("kafka.broker.list")

  def createKafkaProducer: KafkaProducer[String, String] = {
    val p: Properties = new Properties()
    p.put("bootstrap.servers", brokerList)
    //key的序列化类型
    p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //value的序列化类型
    p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //开启幂等性 （kafka生产者保证精准一次性：ACK+幂等性）
    p.put("enable.idempotence", (true: java.lang.Boolean))
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](p)
    producer
  }

  //指定 topic 分区，消息
  def send(topic: String, msg: String) = {
    val producer: KafkaProducer[String, String] = this.createKafkaProducer
    if (producer != null) {
      producer.send(new ProducerRecord[String, String](topic, msg))
    }
  }


  //指定topic\分区、消息
  def send(topic: String, key: String, msg: String) = {
    val producer: KafkaProducer[String, String] = this.createKafkaProducer
    if (producer != null) {
      producer.send(new ProducerRecord[String, String](topic, key, msg))
    }
  }


}
