package com.qihoo.finance.tap.data.convert

import com.alibaba.fastjson.JSONObject
import com.qihoo.finance.tap.{ImportCommon, ScalaHelper}
import org.apache.commons.codec.digest.DigestUtils
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{RowFactory, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object MobileConvertToCsv {

  val logger: Logger = LogManager.getLogger("MobileConvertToCsv")

  val usage =
    """
    Usage: MobileConvertToCsv [--outputFile]  E:\360_doc\lolth\mobile.csv
  """

  type OptionMap = Map[Symbol, Any]


  def main(args: Array[String]) {
    if (args.length == 0) {
      println(usage)
      System.exit(0)
    }

    val argList = args.toList
    val options = ImportCommon.nextOption(Map(), argList)

    val conf = new SparkConf().setAppName("MobileConvertToCsv")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
//    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val txtFile = sc.textFile(options.getOrElse('importFile, "").asInstanceOf[String])
    val outputFile = options.getOrElse('outputFile, "").asInstanceOf[String]

    val headerList = Array( "name", "nm_pass", "nm_sha1", "is_register", "is_risk", "is_internal", "is_service", "merchant_name", "status", "suspect_risk", "overdue_status")

    val dataRdd = txtFile.map {
      line =>
        val jsonObject: JSONObject = ScalaHelper.parseVertexLineGetIdAndAttr(line)

        val nameValue = jsonObject.getString("name")
        // 加密信息
        val encrypt = nameValue
        val sha1Hex = DigestUtils.sha1Hex(nameValue)

        RowFactory.create(nameValue, encrypt, sha1Hex,
          jsonObject.getString("is_register"),
          jsonObject.getString("is_risk"),
          jsonObject.getString("is_internal"),
          jsonObject.getString("is_service"),
          jsonObject.getString("merchant_name"),
          jsonObject.getInteger("status"),
          jsonObject.getInteger("suspect_risk"),
          jsonObject.getInteger("overdue_status")
        )
    }
    var structType = new StructType()

    for ((elem, i) <- headerList.view.zipWithIndex) {
      if (List("status", "suspect_risk", "overdue_status").contains(elem)) {
        structType = structType.add(headerList(i), IntegerType, nullable = true)
      } else {
        structType = structType.add(headerList(i), StringType, nullable = true)
      }
    }

    val df = sqlContext.createDataFrame(dataRdd, structType)

    df.createOrReplaceTempView("mobile_csv_df")

    sqlContext.sql("DROP TABLE IF EXISTS migrate_mobile_tmp")
    sqlContext.sql("create table migrate_mobile_tmp as select * from mobile_csv_df")
    //        df.show()
    //    ScalaHelper.saveAsCSV(outputFile, df)

    println("***********************stoped***********************")
    sc.stop()
  }

}
