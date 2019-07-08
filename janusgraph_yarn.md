# Janusgraph Yarn Configuration

此文档旨在说明 JanusGraph 如何集成 yarn

## 环境路径
CDH的安装目录 /opt/cloudera/parcels/CDH/
CDH的配置文件目录 /etc/hadoop

## 下载
spark-2.2.1-bin-hadoop2.7
janusgraph-0.3.2-hadoop2


## Jar 包冲突解决
spark-2.2.1-bin-hadoop2.7 依赖的 guava-14.0.1.jar 与
janusgraph 依赖的 guava-18.0.jar 存着冲突。使用 guava-18.0.jar

rm -f spark-2.2.1-bin-hadoop2.7/jars/guava-*.jar 

cp janusgraph/lib/guava-18.0.jar spark/jars/

## 修改 bin/gremlin.sh
```bash
export CLASSPATH="$CLASSPATH:/etc/hadoop/conf/*:/opt/cloudera/parcels/CDH/lib/hadoop-yarn/*:/home/q/spark/jars/*"
```

## 文件配置
gremlin_yan.sh
```bash
#!/bin/bash
export HADOOP_CONF_DIR=/etc/hadoop/conf

export CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR
# 关键，会从此目录加载依赖的 spark和yarn jar包 janusgraph 提供的spark jar包不全
export SPARK_HOME=/home/q/spark

export PATH=$PATH:$SPARK_HOME/bin
bin/gremlin.sh
```