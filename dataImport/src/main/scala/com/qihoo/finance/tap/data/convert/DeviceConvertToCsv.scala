package com.qihoo.finance.tap.data.convert

import com.alibaba.fastjson.JSONObject
import com.qihoo.finance.tap.{ImportCommon, ScalaHelper}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object DeviceConvertToCsv {

  val logger: Logger = LogManager.getLogger("DeviceConvertToCsv")

  val usage =
    """
    Usage: DeviceConvertToCsv [--label] [--outputFile] E:\360_doc\lolth\mobile.csv
  """

  type OptionMap = Map[Symbol, Any]


  def main(args: Array[String]) {
    if (args.length == 0) {
      println(usage)
      System.exit(0)
    }

    val argList = args.toList
    val options = ImportCommon.nextOption(Map(), argList)


    val conf = new SparkConf().setAppName("DeviceConvertToCsv")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    //    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val txtFile = sc.textFile(options.getOrElse('importFile, "").asInstanceOf[String])
    val outputFile = options.getOrElse('outputFile, "").asInstanceOf[String]
    val deviceType = options.getOrElse('deviceType, "").asInstanceOf[String]
    if (deviceType == null) {
      println("--deviceType 不能为空 device|wifi")
      System.exit(0)
    }

    val headerList = Array("name", "is_exception", "is_white")

    //    name:ID(human)	age:Int
    val dataRdd = txtFile.map {
      line =>
        val jsonObject: JSONObject = ScalaHelper.parseVertexLineGetIdAndAttr(line)
        RowFactory.create(jsonObject.getString("name"),
          jsonObject.getString("is_exception"),
          jsonObject.getString("is_white")
        )
    }
    var structType = new StructType()

    for ((elem, i) <- headerList.view.zipWithIndex) {
      structType = structType.add(headerList(i), StringType, nullable = true)
    }

    val df = sqlContext.createDataFrame(dataRdd, structType)

    df.createOrReplaceTempView("device_csv_df")

    sqlContext.sql("DROP TABLE IF EXISTS migrate_" + deviceType + "_tmp")
    sqlContext.sql("create table migrate_" + deviceType + "_tmp as select * from device_csv_df")

    //    df.show()
    //    ScalaHelper.saveAsCSV(outputFile, df)

    println("***********************stoped***********************")
    sc.stop()
  }


}
