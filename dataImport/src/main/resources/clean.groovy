graph = JanusGraphFactory.open('conf/gremlin-server/janusgraph-hbase-es-server-new.properties')
graph.close(); org.janusgraph.core.util.JanusGraphCleanup.clear(graph)

:load data/lolth-schema-pass.groovy
graph = JanusGraphFactory.open('conf/gremlin-server/janusgraph-hbase-es-server-new.properties')

defineLolthSchema(graph)
graph.close()