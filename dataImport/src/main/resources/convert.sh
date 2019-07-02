#!/bin/bash
spark-submit --class com.qihoo.finance.tap.data.convert.MobileConvertToCsv --master yarn --conf spark.network.timeout=300 --deploy-mode client --queue root.graph ./dataImport-libs.jar  hdfs://360jinrongbdp/user/finloan/janusgraph_new/mobile_split/part-*


spark-submit --class com.qihoo.finance.tap.data.convert.MergeNodesAndEdges --master yarn --conf spark.network.timeout=600 --deploy-mode client  --queue root.graph ./dataImport-libs.jar --outputFile hdfs://360jinrongbdp/user/finloan/janusgraph_new/merge_all_relation.txt
