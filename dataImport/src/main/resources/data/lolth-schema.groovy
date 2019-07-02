/* lolth-schema.groovy
 *
 * Helper functions for declaring JanusGraph schema elements
 * (vertex labels, edge labels, property keys) to accommodate
 * TP3 sample data.
 *
 * Sample usage in a gremlin.sh session:
 * bin/gremlin.sh
 * :load data/lolth-schema.groovy
 * t = JanusGraphFactory.open('conf/gremlin-server/janusgraph-hbase-es-server.properties')
 * defineLolthSchema(t)
 * t.close()
 * gremlin>
 */

def defineLolthSchema(janusGraph) {
    mgmt = janusGraph.openManagement()
    name = mgmt.makePropertyKey("name").dataType(String.class).make()
    is_register = mgmt.makePropertyKey("is_register").dataType(String.class).make()
    is_risk = mgmt.makePropertyKey("is_risk").dataType(String.class).make()
    is_internal = mgmt.makePropertyKey("is_internal").dataType(String.class).make()
    is_service = mgmt.makePropertyKey("is_service").dataType(String.class).make()
    merchant_name = mgmt.makePropertyKey("merchant_name").dataType(String.class).make()
    is_exception = mgmt.makePropertyKey("is_exception").dataType(String.class).make()
    is_white = mgmt.makePropertyKey("is_white").dataType(String.class).make()

    // name 属性加密
    // nm_pass = mgmt.makePropertyKey("nm_pass").dataType(String.class).make()
    // nm_sha1 = mgmt.makePropertyKey("nm_sha1").dataType(String.class).make()

    status = mgmt.makePropertyKey("status").dataType(Integer.class).make()
    suspect_risk = mgmt.makePropertyKey("suspect_risk").dataType(Integer.class).make()
    overdue_status = mgmt.makePropertyKey("overdue_status").dataType(Integer.class).make()
    mgm = mgmt.makePropertyKey("mgm").dataType(Integer.class).make()

    blid = mgmt.makePropertyKey("bulkLoader.vertex.id").dataType(Long.class).make()
    mgmt.buildIndex("byBulkLoaderVertexId", Vertex.class).addKey(blid).buildCompositeIndex()

    // 注意 JanusGraph 的 label名称区分大小写，而 AgensGraph 不做区分
    // 所有统一使用大写
    mgmt.makeVertexLabel("DEVICE").make()
    mgmt.makeVertexLabel("MOBILE").make()
    mgmt.makeVertexLabel("WIFI").make()

    mgmt.makeEdgeLabel("CALL").multiplicity(Multiplicity.SIMPLE).make()
    mgmt.makeEdgeLabel("HAS").multiplicity(Multiplicity.SIMPLE).make()
    mgmt.makeEdgeLabel("USE").multiplicity(Multiplicity.SIMPLE).make()
    mgmt.makeEdgeLabel("USE_WIFI").multiplicity(Multiplicity.SIMPLE).make()

    mgmt.buildIndex("name", Vertex.class).addKey(name).unique().buildCompositeIndex()
    // mgmt.buildIndex("nm_sha1", Vertex.class).addKey(nm_sha1).unique().buildCompositeIndex()
    mgmt.commit()
}