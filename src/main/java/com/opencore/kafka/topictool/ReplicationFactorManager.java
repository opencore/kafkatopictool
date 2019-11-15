/**
 * Copyright © 2019 Sönke Liebau (soenke.liebau@opencore.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.opencore.kafka.topictool;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.opencore.kafka.ScalaInterface;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

public class ReplicationFactorManager {

  Properties adminClientProperties;
  AdminZkClient adminZkClient = null;
  KafkaZkClient zkClient = null;
  AdminClient adminClient = null;
  boolean rackAware = false;
  Gson gson;
  ReassignmentPlan plannedOperations;
  Collection<Node> liveNodes = null;

  public ReplicationFactorManager(Properties adminClientProperties) {
    this.gson = new GsonBuilder().create();
    this.adminClientProperties = adminClientProperties;

    adminClient = TopicTool.getAdminClient(adminClientProperties);

    // Retrieve list of live brokers
    try {
      liveNodes = adminClient.describeCluster().nodes().get();
    } catch (InterruptedException | ExecutionException e) {
      System.out.println("Exception ocurred when retrieving list of live brokers from cluster, aborting operation: " + e.getMessage());
      return;
    }

    // If there is a node that has no rack assigned we will not use rack aware assignments
    this.rackAware = liveNodes.stream().anyMatch(e -> !e.hasRack());

    if (adminClientProperties.containsKey("zookeeper.connect")) {
      adminZkClient = new AdminZkClient(zkClient);
    }
  }

  public void addOperation(TopicDescription topic, short targetReplicationFactor) {
    // Check if there are enough nodes to satisfy the requested replication factor, otherwise skip topic
    if (targetReplicationFactor > liveNodes.size()) {
      System.out.println(
          "Requested replication factor of " + targetReplicationFactor + " for topic " + topic
              .name() + " is larger than number of available brokers (" + liveNodes.size()
              + ") - skipping topic!");
      return;
    }

    String topicName = topic.name();
    List<TopicPartitionInfo> partitions = topic.partitions();

    // Initialize new operation queue, if there is none (last call was execute(), or no operation added yet)
    if (plannedOperations == null) {
      plannedOperations = new ReassignmentPlan();
      plannedOperations.partitions = new ArrayList<>();
    }

    for (TopicPartitionInfo info : partitions) {
      if (info.replicas().size() > targetReplicationFactor) {
        // Just remove enough replicas to get the factor to match
        Partition partitionChange = new Partition();
        partitionChange.partition = info.partition();
        partitionChange.topic = topicName;
        partitionChange.replicas = info.replicas().stream().map(e -> e.id())
            .collect(Collectors.toList()).subList(0, targetReplicationFactor);
        plannedOperations.partitions.add(partitionChange);
      } else if (info.replicas().size() < targetReplicationFactor) {
        // Create list of nodes that do not currently hold a replica
        List<Node> availableNodes = liveNodes.stream().filter(e -> !info.replicas().contains(e))
            .collect(Collectors.toList());

        Partition partitionChange = new Partition();
        partitionChange.partition = info.partition();
        partitionChange.topic = topicName;

        // Select nodes from that list until replication factor is satisfied
        List<Integer> selectedNodes = availableNodes.stream().map(e -> e.id())
            .collect(Collectors.toList())
            .subList(0, targetReplicationFactor - info.replicas().size());
        partitionChange.replicas = info.replicas().stream().map(e -> e.id())
            .collect(Collectors.toList());
        partitionChange.replicas.addAll(selectedNodes);
        plannedOperations.partitions.add(partitionChange);
      }
    }

    //gson.toJson(plannedOperations, System.out);

  }

  public void clearOperations() {
    this.plannedOperations = null;
  }

  public void executeOperations() {
    if (plannedOperations == null) {
      return;
    }
    if (!adminClientProperties.containsKey("zookeeper.connect")) {
      System.out.println("Changing the replication factor requires configuring a Zookeeper connection, skipping this action.");
      return;
    }
    ScalaInterface scalaInterface = new ScalaInterface(TopicTool.getAdminClient(adminClientProperties),
        adminClientProperties.getProperty("zookeeper.connect"));
    scalaInterface.execute(gson.toJson(plannedOperations));
    plannedOperations = null;
  }

  public boolean isConnected() {
    return zkClient != null && adminZkClient != null;
  }

  private class ReassignmentPlan {

    int version = 1;
    List<Partition> partitions;
  }

  private class Partition {

    String topic;
    int partition;
    List<Integer> replicas;
  }
}
