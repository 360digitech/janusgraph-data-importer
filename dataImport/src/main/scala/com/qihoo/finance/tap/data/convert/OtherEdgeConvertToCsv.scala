package com.qihoo.finance.tap.data.convert

import com.qihoo.finance.tap.ImportCommon
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.tinkerpop.gremlin.driver.Client

object OtherEdgeConvertToCsv {
  val logger: Logger = LogManager.getLogger("OtherEdgeConvertToCsv")

  val usage =
    """
    Usage: OtherEdgeConvertToCsv [--janusgraph-hosts 10.94.90.121] [--janusgraph-port 8182] E:\360_doc\lolth\call_edge.csv
  """

  def main(args: Array[String]) {
    if (args.length == 0) {
      println(usage)
      System.exit(0)
    }

    val argList = args.toList
    val options = ImportCommon.nextOption(Map(), argList)

    val conf = new SparkConf().setAppName("OtherEdgeConvertToCsv")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
//    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val txtFile = sc.textFile(options.getOrElse('importFile, "").asInstanceOf[String])

    val fromLabel = options.getOrElse('fromLabel, null).asInstanceOf[String]
    val toLabel = options.getOrElse('toLabel, null).asInstanceOf[String]
    val edgeType = options.getOrElse('edgeType, null).asInstanceOf[String]

    if (fromLabel == null || toLabel == null) {
      println("必须添加参数 --from-label DEVICE|WIFI|MOBILE --to-label DEVICE|WIFI|MOBILE")
      System.exit(0)
    }

    //      :START_ID(god)	:END_ID(titan)
    //    jupiter	saturn

    val dataRdd = txtFile.map {
      line =>
        val fields = line.replace("\"", "").split(",")
        // "1870276152746","CALL","18602761525746"
        // "13512340050","CALL","15607804358",1
        // CALL 边有 mgm 属性
        RowFactory.create(fields(0), fields(2))
    }

    val structType = new StructType()
      .add(StructField("start_name", StringType, nullable = true))
      .add(StructField("end_name", StringType, nullable = true))

    val df = sqlContext.createDataFrame(dataRdd, structType)
    //    df.show()

    df.createOrReplaceTempView("edge_csv_df")

    sqlContext.sql("DROP TABLE IF EXISTS migrate_" + edgeType + "_tmp")
    sqlContext.sql("create table migrate_" + edgeType + "_tmp as select * from edge_csv_df")


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
