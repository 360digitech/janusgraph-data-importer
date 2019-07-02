
graph = GraphFactory.open("conf/hadoop-graph/hadoop-vertex-script.properties")
blvp = BulkLoaderVertexProgram.build().bulkLoader(OneTimeBulkLoader).writeGraph("conf/gremlin-server/janusgraph-hbase-es-server-new.properties").create(graph)
graph.compute(SparkGraphComputer).program(blvp).submit().get()