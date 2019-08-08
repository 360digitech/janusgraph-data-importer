# JanusGraph 查询优化

JanusGraph 的查询有比较多的优化点，在此做些说明

## 属性 _multiPreFetch 优化
这是个人认为最重要的优化，0.4.0 版本才提供的功能，没有这个功能 JanusGraph 在大数据量的生产环境基本不可用  

```bash
g.V().has('name', P.within('186xxxx6666')).both('CALL').or(has('is_register','true'), has('is_risk','true')).as('m2').profile() 
```
类似上面的语句，没有这个优化，Jansugraph 会找到对端的顶点然后每个顶点单独去获取属性再做过滤条件  
在生产获取的顶点数很多的时候基本不可用
耗时特别长  
触发了这个优化的话它会批量获取顶点的属性然后做过滤  
```bash
gremlin> g.V(6554048).outE('aggregation').otherV().has('name', neq('bob')).count().profile()
==>Traversal Metrics
Step                                                               Count  Traversers       Time (ms)    % Dur
=============================================================================================================
GraphStep(vertex,[6554048])                                            1           1          35.538     0.15
JanusGraphVertexStep(OUT,[aggregation],vertex)                     30159       30159        2220.394     9.28
    \_condition=(PROPERTY AND visibility:normal)
    \_orders=[]
    \_isFitted=true
    \_isOrdered=true
    \_query=org.janusgraph.diskstorage.keycolumnvalue.SliceQuery@8019d62e
    \_multi=true
    \_vertices=20000
    \_multiPreFetch=true
  optimization                                                                                82.480
  backend-query                                                    30159                     275.560
    \_query=org.janusgraph.diskstorage.keycolumnvalue.SliceQuery@81bebe6b
  optimization                                                                                 0.712
  backend-query                                                   257398                    1491.029
    \_query=org.janusgraph.diskstorage.keycolumnvalue.SliceQuery@8019d62e
HasStep([name.neq(bob)])                                           28054       28054       21612.923    90.33
CountGlobalStep                                                        1           1          56.938     0.24
                                            >TOTAL                     -           -       23925.795        -
```
在profile `_multiPreFetch=true` 表示触发了这个优化  
这个优化触发的条件有点苛刻  
首先需要在配置文件中配置  `query.batch-property-prefetch=true`   
其次需要利用`has`进行属性过滤  
再者查询出来的数据行数不能超过 配置文件中 `cache.tx-cache-size` 设置的值（默认值为2W）  
意思是如果查询出点超过设置值就不会触发这个优化  
更加详细的信息可以参考如下[链接](https://github.com/JanusGraph/janusgraph/issues/984)

## 返回结果优化
```bash
g.V().has("MOBILE", "name", P.within('186xxxx6666')).as("m1").both("CALL").as('m2') \
.select("m1", "m2") \
.by(valueMap("name")) \
.by(valueMap("name", "is_risk", "status", "is_service", "overdue_status"))
```
上面的查询返回的结果是 
```bash
{m1={name=[18658606666]}, m2={name=[13064767986]}}
{m1={name=[18658606666]}, m2={name=[13291676581]}}
{m1={name=[18658606666]}, m2={name=[13566665915]}}
{m1={name=[18658606666]}, m2={name=[15072770149]}}
{m1={name=[18658606666]}, m2={name=[15268898802]}}
{m1={name=[18658606666]}, m2={name=[18657617779], status=[3]}}
```
这样的查询返回结果看似没啥问题，我们之前也是这样写的，这样查询语法比较简洁。  
这个查询有两个问题，我们生产的数据量比较大，并且涉及大量的查询。在生产环境应用的内存很快被打满  
这个查询有两个问题，第一个问题是 name返回的是list，在`Java`中 `ArrayList`的默认值是 10，意思是即使你属性只有一个值
也会创建10个对象  
第二个问题是，返回的Map 过多，m1, m2 这两个Map虽然只有一个key，但是在`Java` 中`HashMap`的默认值 16，
同上面的问题会导致大量的内存浪费
```java
public class ArrayList<E> extends AbstractList<E>
        implements List<E>, RandomAccess, Cloneable, java.io.Serializable
{
    private static final long serialVersionUID = 8683452581122892189L;

    /**
     * Default initial capacity.
     */
    private static final int DEFAULT_CAPACITY = 10;
    ...
}


public class HashMap<K,V> extends AbstractMap<K,V>
    implements Map<K,V>, Cloneable, Serializable {

    private static final long serialVersionUID = 362498820763181265L;
    
    /**
     * The default initial capacity - MUST be a power of two.
     */
    static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16
    ...
}
```
正确的查询语句应该是像下面这样的
```bash
g.V().has("MOBILE", "name", P.within('186xxxx6666')).as("m1").both("CALL").as('m2') \
.select("m1", "m2") \
.project("cName", "mobile", "isRisk", "isService") \
.by(select("m1").by(coalesce(values("name"), constant("null"))) ) \
.by(select("m2").by(coalesce(values("name"), constant("null"))) ) \
.by(select("m2").by(coalesce(values("is_risk"), constant("null"))) ) \
.by(select("m2").by(coalesce(values("is_service"), constant("null"))) ) \
# 返回结果为一个 map 并且结果不为 list
{cName=186xxxx6666, mobile=186xxxx6666, isRisk=true, status=0}
```
如果确实需要使用`valueMap` 并且不希望返回 List结果可以用下面的语法
```bash
valueMap().by(unfold())
```
## 插入顶点和边的重复检查
```bash
g.V().has("name", nodeName).fold().coalesce(unfold(), addV("MOBILE").property("name", nodeName)).next();
```
上面的语句类似merge功能，如果顶点不存在添加顶点

```bash
g.V(fromNode).as("a").V(toNode).coalesce(inE(relationLabel).where(outV().as("a")), addE(relationLabel).from("a"))
```
上面的语句会检查两个顶点之间的边是否存着，如果不存在添加对应的边
