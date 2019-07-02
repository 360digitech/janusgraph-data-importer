package com.qihoo.finance.tap

import java.util
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

import com.alibaba.fastjson.JSONObject
import org.apache.log4j.{LogManager, Logger}
import org.apache.tinkerpop.gremlin.driver.{Client, Result}

import scala.util.control.Breaks


object ImportCommon {
  val logger: Logger = LogManager.getLogger("ImportCommon")

  type OptionMap = Map[Symbol, Any]

  def getJanusGraph(hosts: String, port: Int, poolSize: Int): JanusGraphProvider = {
    new JanusGraphProvider(hosts, port, poolSize)
  }

  def isEmpty(x: String) = Option(x).forall(_.isEmpty)


  def submitWithRetry(client: Client, runCql: String) = {
    val loop = new Breaks
    loop.breakable {
      for (a <- 1 to 100) {
        try {
          client.submit(runCql).stream().forEach(new Consumer[Result] {
            override def accept(t: Result): Unit =
              logger.info(t.getLong)
          })
          loop.break()
        } catch {
          case ex: Exception =>
            logger.warn(runCql)
            logger.warn(ex.getMessage, ex)
            TimeUnit.MILLISECONDS.sleep(1000 * a)
        }

      }
    }
  }


  def getResultWithRetry(client: Client, runCql: String): util.List[Result] = {
    var results: util.List[Result] = null
    val loop = new Breaks
    loop.breakable {
      for (a <- 1 to 100) {
        try {
          results = client.submit(runCql).all.get
          loop.break()
        } catch {
          case ex: Exception =>
            logger.warn(ex.getMessage, ex)
            TimeUnit.MILLISECONDS.sleep(1000 * a)
        }
      }
    }

    results
  }


  def handleVertexList(recordList: List[(String, String)], client: Client): Unit = {
    var runCql = "g = graph.traversal();g"

    recordList.foreach { case (label, attrString) =>
      runCql += ".addV('" + label + "')"
      runCql += Helper.buildVertexProperty(label, attrString);
    }

    if (recordList.nonEmpty) {
      runCql += ".count()"

      ImportCommon.submitWithRetry(client, runCql)
    }
  }

  // 顶点增量插入
  def handleVertexIncrementList(recordList: List[(String, String)], client: Client): Unit = {
    var runCql = "g = graph.traversal();g"

    recordList.foreach { case (label, attrString) =>
      val attrJson = Helper.getVertexProperty(attrString)
      val name = attrJson.getString("name")
      runCql += ".V().has('name', '" + name + "').hasLabel('" + label + "').as('m').fold().coalesce(unfold(), addV('" + label + "').property('name', '" + name + "'))"

      runCql += Helper.buildIncrementOtherPropertyString(label, attrJson)
      // status 属性单独进行操作
      val status = attrJson.getIntValue("status")
      if (status > 0) {
        runCql += ".V().has('name', '" + name + "').as('m').where(or(select('m').values('status').is(lt(" + status + ")), select('m').hasNot('status'))).property('status', " + status + ")"
      }
    }

    if (recordList.nonEmpty) {
      runCql += ".count()"
      ImportCommon.submitWithRetry(client, runCql)
    }
  }

  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = s(0) == '-'

    list match {
      case Nil => map
      case "--janusgraph-hosts" :: value :: tail =>
        ImportCommon.nextOption(map ++ Map('janusgraphHosts -> value.toString), tail)
      case "--janusgraph-port" :: value :: tail =>
        ImportCommon.nextOption(map ++ Map('janusgraphPort -> value.toInt), tail)
      case "--batch-size" :: value :: tail =>
        ImportCommon.nextOption(map ++ Map('batchSize -> value.toInt), tail)
      case "--pool-size" :: value :: tail =>
        ImportCommon.nextOption(map ++ Map('poolSize -> value.toInt), tail)
      case "--storage-hostname" :: value :: tail =>
        ImportCommon.nextOption(map ++ Map('storageHostname -> value.toString), tail)
      case "--label" :: value :: tail =>
        ImportCommon.nextOption(map ++ Map('label -> value.toString), tail)
      case "--deviceType" :: value :: tail =>
        ImportCommon.nextOption(map ++ Map('deviceType -> value.toString), tail)
      case "--edgeType" :: value :: tail =>
        ImportCommon.nextOption(map ++ Map('edgeType -> value.toString), tail)
      case "--fromLabel" :: value :: tail =>
        ImportCommon.nextOption(map ++ Map('fromLabel -> value.toString), tail)
      case "--toLabel" :: value :: tail =>
        ImportCommon.nextOption(map ++ Map('toLabel -> value.toString), tail)
      case "--outputFile" :: value :: tail =>
        ImportCommon.nextOption(map ++ Map('outputFile -> value.toString), tail)
      case string :: opt2 :: tail if isSwitch(opt2) =>
        ImportCommon.nextOption(map ++ Map('importFile -> string.toString), list.tail)
      case string :: Nil => ImportCommon.nextOption(map ++ Map('importFile -> string.toString), list.tail)
      case option :: tail => println("Unknown option " + option)
        Map()
    }
  }
}
