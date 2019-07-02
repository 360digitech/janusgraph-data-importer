package com.qihoo.finance.tap;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;

import java.util.Objects;

/**
 * @author zhoupeng
 * @date 2019/1/31
 */
public class JanusGraphProvider {
    private static final Logger logger = LogManager.getLogger(JanusGraphProvider.class);
    public Cluster cluster;


    public JanusGraphProvider(String hosts, int port, int poolSize) {
        Configuration clusterConfig = new PropertiesConfiguration();
        clusterConfig.setProperty("hosts", hosts);
        clusterConfig.setProperty("port", port);
        clusterConfig.setProperty("connectionPool.minSize", poolSize);
        clusterConfig.setProperty("connectionPool.maxSize", poolSize);
        clusterConfig.setProperty("connectionPool.maxInProcessPerConnection", poolSize);
        clusterConfig.setProperty("connectionPool.maxSimultaneousUsagePerConnection", poolSize);
        clusterConfig.setProperty("connectionPool.maxContentLength", 65536000);
        clusterConfig.setProperty("serializer.className", "org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0");
        // 此处很蛋疼，需要返回列表，只能加逗号分隔才行，生成两个类
        clusterConfig.setProperty("serializer.config.ioRegistries",
                "org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry,org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry");

        cluster = Cluster.open(clusterConfig);
    }

    public Client getClient() {
        return this.cluster.connect();
    }


    public void close() throws Exception {
        try {
            if (cluster != null) {
                // the cluster closes all of its clients
                cluster.close();
            }
        } finally {
            cluster = null;
        }
    }

    public void submit(String cql) {
        Client client = this.getClient();
        try {
            client.submit(cql).stream();
        } finally {
            if (!Objects.isNull(client)) {
                client.close();
            }
        }
    }
}