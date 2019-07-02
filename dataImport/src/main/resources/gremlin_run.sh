#!/bin/bash
# 配置 hadoop 环境读取 hdfs文件
export HADOOP_CONF_DIR=/etc/hadoop/conf
export CLASSPATH=$HADOOP_CONF_DIR
nohup bin/gremlin.sh -e ./gremlin_run.groovy &