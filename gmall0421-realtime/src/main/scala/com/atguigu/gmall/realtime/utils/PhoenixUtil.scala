package com.atguigu.gmall.realtime.utils


import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @ClassName: PhoenixUtil
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/16  20:09
 * @Version: 1.0
 */
object PhoenixUtil {


  def main(args: Array[String]): Unit = {
    var sql = "select * from user_stat0421"
    println(queryList(sql).size)

  }


  def queryList(sql: String) = {
    //注册驱动
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    val connection: Connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181")
    val statement: Statement = connection.createStatement()
    val resultSet: ResultSet = statement.executeQuery(sql)
    val metaData: ResultSetMetaData = resultSet.getMetaData
    val listBuffer: ListBuffer[JSONObject] = new ListBuffer[JSONObject]

    while (resultSet.next()) {
      val obj: JSONObject = new JSONObject()
      val columnCount: Int = metaData.getColumnCount
      for (i <- 1 to columnCount) {
        obj.put(metaData.getColumnName(i), resultSet.getString(i))
      }
      listBuffer+=obj
    }
    statement.close()
    connection.close()
    listBuffer.toList
  }

}
