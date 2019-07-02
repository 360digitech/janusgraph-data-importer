# Janusgraph-data-importer

## 说明
本项目是本人具体项目数据迁移过程中的代码，需要根据自身情况修改代码
交流邮箱：zhoupengblack@qq.com

## 工程说明

### resources 文件说明
* conf 目录下放置了数据导入的配置文件，并且对相关配置项都做了些说明   
* data 为图创建的 schema  
* test.data 是我们从 AgensGraph 数据库导出的顶点和边的数据格式 后续的代码都是基于这份数据格式进行解析和操作的    
* hive 将转换后的数据利用 hive 添加唯一 id  
* scripts janusgraph 导入的时候需要对数据进行解析    
* resources 下的 sh,groovy 脚本是方便数据导入 写的一些简单脚本    


### 代码文件说明
* data.convert 目录下代码是将导出的数据转换为hive 表，在利用上面说的hive脚本添加 唯一id  
核心为 MergeNodesAndEdges 利用 spark的 cogroup 操作 转换为 janusgraph 接受的导入格式
此步操作耗时比较久比较占内存，spark 网络超时时间需要 设置长一点

* direct 此目录下的代码 为直接连接 janusgraphServer 插入数据。如果数据量比较小的情况下可以使用

* increment 为导入增量数据，当历史数据导入完毕后需要导入增量数据，需要检查顶点和表是否已经存在


## 常见问题
java.lang.OutOfMemoryError： unable to create new native thread
* 机器的 ulimit -u 设置比较小 可以设置 102400

Caused by: java.lang.OutOfMemoryError: GC overhead limit exceeded
* spark 内存不足，有两个方案 增大 spark.executor.memory 或者调小  spark.executor.cores
保证 spark.executor.memory / spark.executor.cores 在 6G，7G左右

KryoSerializer Failed to find one of the right cookies
* KryoSerializer 序列化 spark 配置不对，参考 hadoop-vertex-script.properties 配置文件

* 确保建议唯一索引的数据是唯一的，id是唯一的，不然数据导入会有问题


## 补充
* pom 文件中的   <scope>provided</scope> 表示在打包的时候不会打入进去
集群环境中是已经有这些包的，在本地调试的时候需要将这行注释掉
本地调试时 代码中的这行也需要放开 //    conf.setMaster("local")
```
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-hive_${scala.version}</artifactId>
    <version>${spark.version}</version>
    <scope>provided</scope>
</dependency>
```


     