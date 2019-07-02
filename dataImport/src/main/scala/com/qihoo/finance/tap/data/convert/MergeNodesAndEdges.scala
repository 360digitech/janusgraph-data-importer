package com.qihoo.finance.tap.data.convert

import com.qihoo.finance.tap.ImportCommon
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object MergeNodesAndEdges {

  val logger: Logger = LogManager.getLogger("MergeNodesAndEdges")

  val usage =
    """
       将顶点和边合并为一行，做批量导入
      Usage: MergeNodesAndEdges --outputFile
  """

  type OptionMap = Map[Symbol, Any]


  def main(args: Array[String]) {
    //    if (args.length == 0) {
    //      println(usage)
    //      System.exit(0)
    //    }

    val argList = args.toList
    val options = ImportCommon.nextOption(Map(), argList)

    val conf = new SparkConf().setAppName("MergeNodesAndEdges")
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    //    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val outputFile = options.getOrElse('outputFile, "").asInstanceOf[String]

    val (mobile_df: DataFrame, device_df: DataFrame, wifi_df: DataFrame, call_df: DataFrame, has_df: DataFrame, use_df: DataFrame, use_wifi_df: DataFrame) =
      generateTestDataDF(sc, sqlContext)

//    val mobile_df = sqlContext.sql("select * from migrate_mobile_id_tmp")
//    val device_df = sqlContext.sql("select * from migrate_device_id_tmp")
//    val wifi_df = sqlContext.sql("select * from migrate_wifi_id_tmp")
//
//    val call_df = sqlContext.sql("select * from migrate_call_id_tmp")
//    val has_df = sqlContext.sql("select * from migrate_has_id_tmp")
//    val use_df = sqlContext.sql("select * from migrate_use_id_tmp")
//    val use_wifi_df = sqlContext.sql("select * from migrate_use_wifi_id_tmp")


    val mobile_kv = mobile_df.rdd.keyBy(_ (0)).mapValues(fields => {
      // label, other props ...
      List("MOBILE",
        replaceNullEmpty(fields(1)),
        replaceNullEmpty(fields(2)),
        replaceNullEmpty(fields(3)),
        replaceNullEmpty(fields(4)),
        replaceNullEmpty(fields(5)),
        replaceNullEmpty(fields(6)),
        replaceNullEmpty(fields(7)),
        replaceNullEmpty(fields(8)),
        replaceNullEmpty(fields(9)),
        replaceNullEmpty(fields(10)),
        replaceNullEmpty(fields(11))
      ).mkString(",")
    })

    val device_kv = device_df.rdd.keyBy(_ (0)).mapValues(fields => {
      // label, other props ...
      ("DEVICE", fields(1), replaceNullEmpty(fields(2)), replaceNullEmpty(fields(3))).productIterator.mkString(",")
    })

    val wifi_kv = wifi_df.rdd.keyBy(_ (0)).mapValues(fields => {
      // label, other props ...
      ("WIFI", fields(1), replaceNullEmpty(fields(2)), replaceNullEmpty(fields(3))).productIterator.mkString(",")
    })

    val call_in_kv = call_df.rdd.keyBy(_ (1)).mapValues(fields => (fields(0), replaceNullEmpty(fields(2))))
    val call_out_kv = call_df.rdd.keyBy(_ (0)).mapValues(fields => (fields(1), replaceNullEmpty(fields(2))))

    val has_in_kv = has_df.rdd.keyBy(_ (1)).mapValues(fields => fields(0))
    val has_out_kv = has_df.rdd.keyBy(_ (0)).mapValues(fields => fields(1))

    val use_out_kv = use_df.rdd.keyBy(_ (0)).mapValues(fields => fields(1))
    val use_in_kv = use_df.rdd.keyBy(_ (1)).mapValues(fields => fields(0))

    val use_wifi_out_kv = use_wifi_df.rdd.keyBy(_ (0)).mapValues(fields => fields(1))
    val use_wifi_in_kv = use_wifi_df.rdd.keyBy(_ (1)).mapValues(fields => fields(0))

    val mobile_result_rdd = mobile_kv.cogroup(call_in_kv).map(v => {
      val callIn = v._2._2.toList.map(v => {
        "CALL," + v._1 + "," + v._2
      }).mkString("|")
      val edge = forceJoinBeforeAndNow(v._2._1.toList.head, callIn, "\t")
      (v._1, edge)
    }).cogroup(has_in_kv).map(v => {
      val hasIn = v._2._2.toList.map(v => {
        "HAS," + v
      }).mkString("|")
      val edge = joinBeforeAndNowWithCheck(v._2._1.toList.head, hasIn, "\t", "|")
      (v._1, edge)
    }).cogroup(call_out_kv).map(v => {
      val callOut = v._2._2.toList.map(v => {
        "CALL," + v._1 + "," + v._2
      }).mkString("|")
      val edge = forceJoinBeforeAndNow(v._2._1.toList.head, callOut, "\t")
      (v._1, edge)
    }).cogroup(use_out_kv).map(v => {
      val useOut = v._2._2.toList.map(v => {
        "USE," + v
      }).mkString("|")
      val edge = joinBeforeAndNowWithCheck(v._2._1.toList.head, useOut, "\t", "|")
      (v._1, edge)
    }).cogroup(use_wifi_out_kv).map(v => {
      val useOut = v._2._2.toList.map(v => {
        "USE_WIFI," + v
      }).mkString("|")
      val edge = joinBeforeAndNowWithCheck(v._2._1.toList.head, useOut, "\t", "|")
      (v._1, edge)
    }).map(v => v._1 + "," + v._2)

    val device_result_rdd = device_kv.cogroup(use_in_kv).map(v => {
      val useIn = v._2._2.toList.map(v => {
        "USE," + v
      }).mkString("|")
      val edge = forceJoinBeforeAndNow(v._2._1.toList.head, useIn, "\t")
      (v._1, edge)
    }).cogroup(has_out_kv).map(v => {
      val hasOut = v._2._2.toList.map(v => {
        "HAS," + v
      }).mkString("|")
      val edge = forceJoinBeforeAndNow(v._2._1.toList.head, hasOut, "\t")
      (v._1, edge)
    }).map(v => v._1 + "," + v._2)

    val wifi_result_rdd = wifi_kv.cogroup(use_wifi_in_kv).map(v => {
      val useIn = v._2._2.toList.map(v => {
        "USE_WIFI," + v
      }).mkString("|")
      val edge = forceJoinBeforeAndNow(v._2._1.toList.head, useIn, "\t")
      // USE_WIFI has't outEdge so add \t
      (v._1, edge + "\t")
    }).map(v => v._1 + "," + v._2)

    val total_result = mobile_result_rdd ++ device_result_rdd ++ wifi_result_rdd

    total_result.saveAsTextFile(outputFile)

    //    total_result.collect().foreach(println)

    println("***********************stoped***********************")
    sc.stop()
  }

  private def generateTestDataDF(sc: SparkContext, sqlContext: SQLContext) = {
    val mobile_rdd = sc.parallelize(Seq(
      Row(1L, "13908125867", "3|TVOiyN2mC/ihdQuMBaw+0A==", "12dd2479ed75af60968d012fa139ff1cffac3683", "true", "", "", "", "", 1, null, null),
      Row(2L, "13908125868", "3|TVOiyN2mC/ihdQuMBaw+0A==", "12dd2479ed75af60968d012fa139ff1cffac3683", "true", "", "", "", "", 1, null, 0),
      Row(3L, "13908125869", "3|TVOiyN2mC/ihdQuMBaw+0A==", "12dd2479ed75af60968d012fa139ff1cffac3683", "true", "", "", "", "", 3, null, 1)
    ))
    val device_rdd = sc.parallelize(Seq(
      Row(11L, "FP13682956455", null, "false"),
      Row(12L, "FP13682956456", "true", null),
      Row(13L, "FP13682956457", "true", "false")
    ))
    val wifi_rdd = sc.parallelize(Seq(
      Row(21L, "bssid13682956455", null, "false"),
      Row(22L, "bssid13682956456", "true", null),
      Row(23L, "bssid13682956457", "true", "false")
    ))

    val call_rdd = sc.parallelize(Seq(
      Row(1L, 2L, null),
      //      Row(3L, 2L, 1),
      Row(2L, 1L, 1)
      //      Row(2L, 3L, 1)
    ))
    val has_rdd = sc.parallelize(Seq(
      Row(11L, 1L),
      Row(11L, 2L),
      Row(11L, 3L),
      Row(12L, 1L),
      Row(12L, 2L)
    ))
    val use_rdd = sc.parallelize(Seq(
      //      Row(1L, 11L),
      //      Row(1L, 12L),
      //      Row(2L, 13L),
      Row(2L, 11L)
      //      Row(3L, 12L)
    ))
    val use_wifi_rdd = sc.parallelize(Seq(
      Row(1L, 21L),
      Row(1L, 22L),
      Row(2L, 23L),
      Row(2L, 21L),
      Row(3L, 22L)
    ))

    val mobile_schema = StructType(List(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("nm_pass", StringType, nullable = true),
      StructField("nm_sha1", StringType, nullable = true),
      StructField("is_register", StringType, nullable = true),
      StructField("is_risk", StringType, nullable = true),
      StructField("is_internal", StringType, nullable = true),
      StructField("is_service", StringType, nullable = true),
      StructField("merchant_name", StringType, nullable = true),
      StructField("status", IntegerType, nullable = true),
      StructField("suspect_risk", IntegerType, nullable = true),
      StructField("overdue_status", IntegerType, nullable = true)
    ))

    val call_schema = StructType(List(
      StructField("start_id", LongType, nullable = false),
      StructField("end_id", LongType, nullable = true),
      StructField("mgm", IntegerType, nullable = true)
    ))
    val edge_schema = StructType(List(
      StructField("start_id", LongType, nullable = false),
      StructField("end_id", LongType, nullable = true)
    ))
    val device_schema = StructType(List(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("is_exception", StringType, nullable = true),
      StructField("is_white", StringType, nullable = true)
    ))

    val mobile_df = sqlContext.createDataFrame(mobile_rdd, mobile_schema)
    val device_df = sqlContext.createDataFrame(device_rdd, device_schema)
    val wifi_df = sqlContext.createDataFrame(wifi_rdd, device_schema)

    val call_df = sqlContext.createDataFrame(call_rdd, call_schema)
    val has_df = sqlContext.createDataFrame(has_rdd, edge_schema)
    val use_df = sqlContext.createDataFrame(use_rdd, edge_schema)
    val use_wifi_df = sqlContext.createDataFrame(use_wifi_rdd, edge_schema)
    (mobile_df, device_df, wifi_df, call_df, has_df, use_df, use_wifi_df)
  }

  def joinBeforeAndNowWithCheck(before: String, now: String, beforSep: String, separate: String): String = {
    var edge: String = null
    if (now.isEmpty) {
      edge = before
    } else if (before.endsWith(beforSep)) {
      edge = List(before, now).mkString("")
    } else {
      edge = List(before, now).mkString(separate)
    }
    edge
  }

  def forceJoinBeforeAndNow(before: String, now: String, separate: String): String = {
    val edge = List(before, now).mkString(separate)
    edge
  }

  def replaceNullEmpty(field: Any): Any = {
    var value = field
    if (value == null) {
      value = ""
    }
    value
  }


}
