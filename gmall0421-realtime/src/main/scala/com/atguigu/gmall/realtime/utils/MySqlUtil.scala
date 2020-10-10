package com.atguigu.gmall.realtime.utils

import java.sql.{Connection, ResultSet, ResultSetMetaData, Statement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import com.alibaba.fastjson.JSONObject
import javax.sql.DataSource

import scala.collection.mutable.ListBuffer

/**
 * @ClassName: MySqlUtil
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/23  18:49
 * @Version: 1.0
 */
object MySqlUtil {


  def queryList(sql: String) = {
    val resultList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]()
    val statement: Statement = getConnect().createStatement()
    val rs: ResultSet = statement.executeQuery(sql)
    val metaData: ResultSetMetaData = rs.getMetaData
    while (rs.next()) {
      val jSONObject: JSONObject = new JSONObject()
      for (i <- 1 to metaData.getColumnCount) {
        jSONObject.put(metaData.getColumnName(i), rs.getObject(i))
      }
      resultList.append(jSONObject)
    }
    resultList
  }





  //通过Druid连接池获取连接
  def getConnect(): Connection = {
    val properties: Properties = new Properties()
    properties.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("druid.properties"))
    val source: DataSource = DruidDataSourceFactory.createDataSource(properties)
    source.getConnection
  }


}
