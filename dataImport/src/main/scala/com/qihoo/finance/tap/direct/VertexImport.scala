package com.qihoo.finance.tap.direct

import com.qihoo.finance.tap.ImportCommon
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object VertexImport {

  val logger: Logger = LogManager.getLogger("VertexImport")

  val usage =
    """
    Usage: VertexImport [--janusgraph-hosts 10.94.90.121] [--janusgraph-port 8182] [--batch-size 20] E:\360_doc\lolth\mobile.csv
  """

  type OptionMap = Map[Symbol, Any]

  def main(args: Array[String]) {
    if (args.length == 0) {
      println(usage)
      System.exit(0)
    }

    val argList = args.toList
    val options = ImportCommon.nextOption(Map(), argList)

    val conf = new SparkConf().setAppName("VertexImport")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
//        conf.setMaster("local")

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
            ImportCommon.handleVertexList(recordList, client)
            recordList = List()
          }
        }
      })

      ImportCommon.handleVertexList(recordList, client)
      client.close()
      provider.close()
    })

    println("***********************stoped***********************")
    sc.stop()
  }


}
