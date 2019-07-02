package com.qihoo.finance.tap.direct

import com.qihoo.finance.tap.ImportCommon
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.tinkerpop.gremlin.driver.Client

object EdgeImport {
  val logger: Logger = LogManager.getLogger("EdgeImport")

  val usage =
    """
    Usage: EdgeImport [--janusgraph-hosts 10.94.90.121] [--janusgraph-port 8182] E:\360_doc\lolth\call_edge.csv
  """

  def main(args: Array[String]) {
    if (args.length == 0) {
      println(usage)
      System.exit(0)
    }

    val argList = args.toList
    val options = ImportCommon.nextOption(Map(), argList)

    val conf = new SparkConf().setAppName("EdgeImport")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    //     conf.setMaster("local")

    val sc = new SparkContext(conf)
    val txtFile = sc.textFile(options.getOrElse('importFile, "").asInstanceOf[String])
    val hosts = options.getOrElse('janusgraphHosts, "").asInstanceOf[String]
    val port = options.getOrElse('janusgraphPort, 0).asInstanceOf[Int]
    val batchSize = options.getOrElse('batchSize, 50).asInstanceOf[Int]
    val poolSize = options.getOrElse('poolSize, 16).asInstanceOf[Int]

    txtFile.map {
      line =>
        val fields = line.replace("\"", "").split(",")
        // "1870276152746","CALL","18602761525746"
        // "13512340050","CALL","15607804358",1
        // CALL 边有 mgm 属性
        if (fields.length == 4 && "CALL".equals(fields(1)) && !"\\N".equals(fields(3))) {
          (fields(0), fields(1), fields(2), Some(fields(3)))
        } else {
          (fields(0), fields(1), fields(2), None)
        }

    }.foreachPartition(partitionOfRecords => {
      val provider = ImportCommon.getJanusGraph(hosts, port, poolSize)
      val client = provider.getClient

      var cqlList: List[String] = List()
      partitionOfRecords.foreach(record => {
        var edgeCql = ""
        if (record._2 == "CALL" && record._4.nonEmpty) {
          edgeCql = ".V().has('name','" + record._1 + "').as('a').V().has('name','" + record._3 + "').addE('" + record._2 + "').from('a').property('mgm'," + record._4.get + ")"
        } else {
          edgeCql = ".V().has('name','" + record._1 + "').as('a').V().has('name','" + record._3 + "').addE('" + record._2 + "').from('a')"
        }
        cqlList = edgeCql :: cqlList
        if (cqlList.size >= batchSize) {
          handleEdgeList(cqlList, client)
          cqlList = List()
        }
      })

      handleEdgeList(cqlList, client)
      client.close()
      provider.close()
    })
    println("***********************stoped***********************")
    sc.stop()
  }

  private def handleEdgeList(cqlList: List[String], client: Client): Unit = {
    var runCql = "g = graph.traversal();g"

    cqlList.foreach(cql => runCql += cql)
    if (cqlList.nonEmpty) {
      runCql += ".count()"
      ImportCommon.submitWithRetry(client, runCql)
    }
  }

}
