package com.opencore.kafka.testutils;

import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;

public class TestCluster extends ClusterTestHarness {

  public TestCluster() {
    super();
  }

  String getBootstrapServers() {
    return bootstrapServers;
  }

  String getZKConnect() {
    return zkConnect;
  }

  Properties getClientConfig() {
    Properties result = new Properties();
    result.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
    result.setProperty("zookeeper.connect", getZKConnect());
    return result;
  }
}
