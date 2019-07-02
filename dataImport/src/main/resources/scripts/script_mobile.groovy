def parse(line) {
    def (vertex, inEdges, outEdges) = line.split(/\t/, 3)
    def (v1id, v1label, v1props) = vertex.split(/,/, 3)
    def v1 = graph.addVertex(T.id, v1id.toLong(), T.label, v1label)
    switch (v1label) {
        case "MOBILE":
            def (name, nm_pass, nm_sha1, is_register, is_risk, is_internal, is_service, merchant_name, status, suspect_risk, overdue_status) = v1props.split(/,/, 11)
            v1.property("name", name)
            v1.property("nm_pass", nm_pass)
            v1.property("nm_sha1", nm_sha1)

            if (is_register?.trim()) {
                v1.property("is_register", is_register)
            }
            if (is_risk?.trim()) {
                v1.property("is_risk", is_risk)
            }
            if (is_internal?.trim()) {
                v1.property("is_internal", is_internal)
            }
            if (is_service?.trim()) {
                v1.property("is_service", is_service)
            }
            if (merchant_name?.trim()) {
                v1.property("merchant_name", merchant_name)
            }
            if (status?.trim()) {
                v1.property("status", status.toInteger())
            }
            if (suspect_risk?.trim()) {
                v1.property("suspect_risk", suspect_risk.toInteger())
            }
            if (overdue_status?.trim()) {
                v1.property("overdue_status", overdue_status.toInteger())
            }
            break
        case "DEVICE":
        case "WIFI":
            def (name, is_exception, is_white) = v1props.split(/,/, 3)
            v1.property("name", name)
            if (is_exception?.trim()) {
                v1.property("is_exception", is_exception)
            }
            if (is_white?.trim()) {
                v1.property("is_white", is_white)
            }
            break
        default:
            throw new Exception("Unexpected vertex label: ${v1label}")
    }
    [[outEdges, true], [inEdges, false]].each { def edges, def out ->
        edges.split(/\|/).grep().each { def edge ->
            def parts = edge.split(/,/)
            def otherV, eLabel, mgm = null
            if (parts.size() == 2) {
                (eLabel, otherV) = parts
            } else {
                (eLabel, otherV, mgm) = parts
            }
            def v2 = graph.addVertex(T.id, otherV.toLong())
            def e = out ? v1.addOutEdge(eLabel, v2) : v1.addInEdge(eLabel, v2)

            if (mgm?.trim()) e.property("mgm", mgm.toInteger())
        }
    }
    return v1
}