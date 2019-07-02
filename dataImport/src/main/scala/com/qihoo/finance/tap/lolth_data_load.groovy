package com.qihoo.finance.tap

import org.apache.tinkerpop.gremlin.structure.T

class lolth_data_load {

    static void main(args) {
        List<String> test = [
                "1,MOBILE,13908125867,3|TVOiyN2mC/ihdQuMBaw+0A==,12dd2479ed75af60968d012fa139ff1cffac3683,true,,,,,1,,\tCALL,2,1|HAS,11|HAS,12\tCALL,2,|USE_WIFI,21|USE_WIFI,22",
                "3,MOBILE,13908125869,3|TVOiyN2mC/ihdQuMBaw+0A==,12dd2479ed75af60968d012fa139ff1cffac3683,true,,,,,3,,1\tHAS,11\tUSE_WIFI,22",
                "2,MOBILE,13908125868,3|TVOiyN2mC/ihdQuMBaw+0A==,12dd2479ed75af60968d012fa139ff1cffac3683,true,,,,,1,,0\tCALL,1,|HAS,11|HAS,12\tCALL,1,1|USE,11|USE_WIFI,23|USE_WIFI,21",
                "13,DEVICE,FP13682956457,true,false\t\t",
                "11,DEVICE,FP13682956455,,false\tUSE,2\tHAS,1|HAS,2|HAS,3",
                "12,DEVICE,FP13682956456,true,\t\tHAS,1|HAS,2",
                "21,WIFI,bssid13682956455,,false\tUSE_WIFI,1|USE_WIFI,2\t",
                "22,WIFI,bssid13682956456,true,\tUSE_WIFI,1|USE_WIFI,3\t",
                "23,WIFI,bssid13682956457,true,false\tUSE_WIFI,2\t"
        ]

        for (String item : test) {
            parse(item)
        }
    }

    // 正式使用只需要 parse 函数
    def  parse(line) {
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

}
