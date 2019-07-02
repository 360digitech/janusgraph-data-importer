package com.qihoo.finance.tap.increment

import java.util

import com.qihoo.finance.tap.{Helper, ImportCommon}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.tinkerpop.gremlin.driver.{Client, Result}

object VertexImportIncrement {

  val logger: Logger = LogManager.getLogger("VertexImportIncrement")

  val usage =
    """
       顶点增量导入，判断属性
    Usage: VertexImportIncrement [--janusgraph-hosts 10.94.90.121] [--janusgraph-port 8182] [--batch-size 20] E:\360_doc\lolth\mobile.csv
  """

  type OptionMap = Map[Symbol, Any]

  def main(args: Array[String]) {
    if (args.length == 0) {
      println(usage)
      System.exit(0)
    }

    val argList = args.toList
    val options = ImportCommon.nextOption(Map(), argList)

    val conf = new SparkConf().setAppName("VertexImportIncrement")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
//    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val txtFile = sc.textFile(options.getOrElse('importFile, "").asInstanceOf[String])
    val hosts = options.getOrElse('janusgraphHosts, "").asInstanceOf[String]
    val port = options.getOrElse('janusgraphPort, 8182).asInstanceOf[Int]
    val batchSize = options.getOrElse('batchSize, 50).asInstanceOf[Int]
    val poolSize = options.getOrElse('poolSize, 16).asInstanceOf[Int]


    txtFile.map {
      line =>
        val labelLast = line.indexOf("[")
        val attrStart = line.indexOf("{")
        val label = line.substring(0, labelLast)
        val attrStr = line.substring(attrStart, line.length)
        (label.toUpperCase(), attrStr)
    }.foreachPartition(partitionOfRecords => {
      val provider = ImportCommon.getJanusGraph(hosts, port, poolSize)
      val client = provider.getClient
      var recordList: List[(String, String)] = List()

      partitionOfRecords.foreach(record => {
        if (!ImportCommon.isEmpty(record._1)) {

            recordList = (record._1, record._2) :: recordList
            if (recordList.size >= batchSize) {
              ImportCommon.handleVertexIncrementList(recordList, client)
              recordList = List()
            }

        }
      })

      ImportCommon.handleVertexIncrementList(recordList, client)
      client.close()
      provider.close()
    })

    println("***********************stoped***********************")
    sc.stop()
  }

  def isVertexExist(record: (String, String), client: Client): Boolean = {
    val jsonObject = Helper.getVertexProperty(record._2)
    val name = jsonObject.getString("name")
    val cql = "g.V().has('name','" + name + "').count()"

    val results: util.List[Result] = ImportCommon.getResultWithRetry(client, cql)
    //    val results: util.List[Result] = client.submit(cql).all.get

    if (results != null && results.size() > 0 && results.get(0).getInt > 0) {
      return true
    }
    false
  }
}
