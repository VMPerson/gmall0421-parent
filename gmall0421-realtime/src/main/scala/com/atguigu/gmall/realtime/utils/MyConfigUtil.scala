package com.atguigu.gmall.realtime.utils

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

/**
 * @ClassName: MyConfig
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/12  10:54
 * @Version: 1.0
 */
object MyConfigUtil {

  def load(path: String): Properties = {
    val properties: Properties = new Properties()
    properties.load(
      new InputStreamReader(
        Thread.currentThread().getContextClassLoader.getResourceAsStream(path),
        StandardCharsets.UTF_8
      ))
    properties
  }

  def main(args: Array[String]): Unit = {
    val properties: Properties = MyConfigUtil.load("config.properties")
    val str: String = properties.getProperty("kafka.broker.list")
    println(str)
  }


}
