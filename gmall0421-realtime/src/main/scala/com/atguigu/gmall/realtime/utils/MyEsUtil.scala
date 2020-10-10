package com.atguigu.gmall.realtime.utils

import java.util

import com.atguigu.gmall.realtime.Movie
import com.atguigu.gmall.realtime.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._

/**
 * @ClassName: MyEsUtils
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/9/11  15:12
 * @Version: 1.0
 */
object MyEsUtil {


  //声明jestClientFactory
  private var jestClientFactory: JestClientFactory = null

  //构建JestClient
  def getClient: JestClient = {
    if (jestClientFactory == null) bulid()
    jestClientFactory.getObject
  }

  //构建clientBuild
  def bulid() = {
    jestClientFactory = new JestClientFactory
    val config: HttpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200")
      .multiThreaded(true)
      .maxTotalConnection(10)
      .readTimeout(5000)
      .connTimeout(10000)
      .build()
    jestClientFactory.setHttpClientConfig(config)
  }


  //通过DSL语法插入
  def PUTIndex() = {
    //获取连接
    val client: JestClient = getClient

    var insertStr: String =
      """
        |{
        |    "id":2,
        |    "name":"湄公河行动",
        |    "doubanScore":8.0,
        |    "actorList":[
        |    {"id":3,"name":"张涵予"}
        |    ]
        |}
        |
    """.stripMargin
    val action: Index = new Index.Builder(insertStr).index("movie_index").id("1").`type`("movie").build()
    client.execute(action)
    //关闭连接
    client.close()
  }

  //通过封装样例类的方式向索引里面添加数据
  def PUTIndexByClass = {
    //获取连接对象
    val client: JestClient = getClient
    val arrayList: util.ArrayList[util.Map[String, Any]] = new util.ArrayList[util.Map[String, Any]]()
    val map: util.HashMap[String, Any] = new util.HashMap[String, Any]()
    map.put("id", 6)
    map.put("name", "张艺谋")
    arrayList.add(map)
    val map2: util.HashMap[String, Any] = new util.HashMap[String, Any]()
    map2.put("id", 8)
    map2.put("name", "黄轩")
    arrayList.add(map2)

    val action: Index = new Index.Builder(Movie(25, "芳华", 9.2, arrayList))
      .index("movie_movie")
      .id("9")
      .`type`("movie")
      .build()

    //执行添加方法
    client.execute(action)
    //关闭资源
    client.close()
  }


  //查询指定索引指定文档id数据
  def QueryData(): Unit = {
    //获取连接
    val client: JestClient = getClient
    val get: Get = new Get.Builder("movie_movie", "2").build()
    val result: DocumentResult = client.execute(get)
    val string: String = result.getJsonString
    println(string)
    //关闭连接
    client.close()
  }


  /**
   * 根据索引名，查询该索引下的所有数据
   *
   * @param indexName 索引名
   */
  def SearchData(indexName: String): Unit = {
    //建立客户端
    val client: JestClient = getClient
    var searthString: String =
      """
        |{
        |  "query": {
        |    "match_all": {}
        |  }
        |}
        |
        |""".stripMargin

    val result: SearchResult = client.execute(new Search.Builder(searthString)
      .addIndex(indexName)
      .build())
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    //将java的list转化为scala的list，需要引入隐士转换类
    import scala.collection.JavaConverters._
    val resList: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
    println(resList.mkString("\n"))
    //关闭客户端
    client.close()
  }


  //批量插入
  def bulkInsert(dauList: List[(String, Any)], indexName: String): Unit = {
    val client: JestClient = getClient
    //创建批量插入对象
    val builder: Bulk.Builder = new Bulk.Builder
    for ((mid, duaInfo) <- dauList) {
      val index: Index = new Index.Builder(duaInfo).id(mid).index(indexName).`type`("_doc").build()
      //向批量对象中添加元素
      builder.addAction(index)
    }

    val bulk: Bulk = builder.build()
    val response: BulkResult = client.execute(bulk)
    //获取插入成功的条数
    /*    val items: util.List[BulkResult#BulkResultItem] = response.getItems
        println("向ES中成功插入：" + items.size() + " 条数据")*/
    //关闭连接
    client.close()

  }


}

