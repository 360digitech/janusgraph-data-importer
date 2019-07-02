package com.qihoo.finance.tap

import java.lang

import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.DataFrame

object ScalaHelper {
  def convertHeader(label: String, headerMap: Map[String, String], headerList: Array[String]): Map[String, String] = {
    var headResult = Map[String, String]()
    for (field <- headerList) {
      var result: String = null

      if ("name".equals(field)) {
        result = "%s:ID(%s)".format(field, label)
      } else {
        if (headerMap.contains(field)) {
          result = field + ":" + headerMap(field)
        } else {
          result = field
        }
      }

      headResult = headResult + (field -> result)
    }

    headResult
  }


  def saveAsCSV(outputFile: String, df: DataFrame) = {
    df.repartition(1)
      .write
      .mode("overwrite")
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("treatEmptyValuesAsNulls", "false")
      .save(outputFile)
  }


  def parseVertexLineGetIdAndAttr(line: String) = {
    val labelLast = line.indexOf("[")
    val idLast = line.indexOf("]")
    val attrStart = line.indexOf("{")
    val attrStr = line.substring(attrStart, line.length)

    val jsonObject = Helper.getVertexProperty(attrStr)
    jsonObject
  }

}
