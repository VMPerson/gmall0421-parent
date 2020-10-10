package com.atguigu.gmall.realtime

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._

/**
 * @ClassName: MyDemo
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/11  20:54
 * @Version: 1.0
 */
object MyDemo {

  private var jestClientFactory: JestClientFactory = null


  //构建jestClient
  def build() = {
    jestClientFactory = new JestClientFactory
    jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200")
      .maxTotalConnection(10)
      .multiThreaded(true)
      .connTimeout(10000)
      .readTimeout(5000)
      .build())
  }

  //获取jestClient
  def getClient = {
    if (jestClientFactory == null) build()
    jestClientFactory.getObject
  }


  //插入数据 通过DSL语法的方式
  def PUTByDSL: Unit = {
    //获取连接
    val client: JestClient = getClient
    var str: String =
      """
        | {
        |    "id":2,
        |    "name":"唐人街探案",
        |    "doubanScore":8.7,
        |    "actorList":[
        |    {"id":12,"name":"王宝强"}
        |    {"id":9,"name":"李易峰"}
        |    ]
        |  }
        |""".stripMargin
    val action: Index = new Index.Builder(str)
      .index("movie_movie")
      .id("10")
      .`type`("movie")
      .build()
    client.execute(action)
    //关闭连接
    client.close()
  }


  def PUTByClass = {
    val client: JestClient = getClient

    val list: util.ArrayList[util.Map[String, Any]] = new util.ArrayList[util.Map[String, Any]]()
    val map1: util.HashMap[String, Any] = new util.HashMap[String, Any]()
    map1.put("id", "90")
    map1.put("name", "洪真英")
    list.add(map1)
    val map2: util.HashMap[String, Any] = new util.HashMap[String, Any]()
    map2.put("id", "34")
    map2.put("name", "宋菲")
    list.add(map2)

    val index: Index = new Index.Builder(Movie(100, "打糕舞", 8.9, list)).index("movie_movie")
      .id("16")
      .`type`("movie")
      .build()
    client.execute(index)
    client.close()
  }

  //普通查询 指定索引指定ID采用GET
  def queryInfo = {
    val client: JestClient = getClient
    val result: DocumentResult = client.execute(new Get.Builder("movie_movie", "16").build())
    println(result.getJsonString)
    client.close()
  }


  //查询多个的话，采用search

  def searchInfo = {
    val client: JestClient = getClient

    var str: String =
      """
        |{
        |  "query": {
        |    "match_all": {}
        |  }
        |}
        |
        |""".stripMargin

    val search: Search = new Search.Builder(str).addIndex("movie_movie").build()
    val result: SearchResult = client.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    //将Java的list转化为scala的list
    import scala.collection.JavaConverters._
    val list1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
    println(list1.mkString("\n"))
    client.close()
  }


  def main(args: Array[String]): Unit = {
    // PUTByDSL
    // PUTByClass
    // queryInfo
    searchInfo

  }

}

case class Movie(
                  id: Long,
                  name: String,
                  doubanScore: Double,
                  actorList: java.util.List[java.util.Map[String, Any]]
                )


/**



    joinWithSplitAmountDStream.map(elem => {
      JSON.toJSONString(elem, new SerializeConfig(true))
    }).print(100)

 **/


