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

import com.opencore.kafka.topictool.comparison.CompactionCompareThread;
import com.opencore.kafka.topictool.comparison.DeletionCompareThread;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicComparer {

  Map<String, Properties> clusterPropertiesMap;
  Map<String, KafkaConsumer<Byte[], Byte[]>> consumerMap;
  Map<String, TopicManager> managerMap;
  Map<String, AdminClient> adminClientMap;
  ExecutorService executor = null;
  private Logger logger = LoggerFactory.getLogger(TopicComparer.class);


  public TopicComparer(Map<String, Properties> clusterPropertiesMap, int threadCount) {
    this.clusterPropertiesMap = clusterPropertiesMap;
    this.managerMap = new HashMap<>();
    this.adminClientMap = new HashMap<>();
    this.executor = Executors.newFixedThreadPool(threadCount);

    for (String clusterName : clusterPropertiesMap.keySet()) {
      Properties clusterProps = clusterPropertiesMap.get(clusterName);
      managerMap.put(clusterName, new TopicManager(clusterName, clusterProps));
      adminClientMap.put(clusterName, TopicTool.getAdminClient(clusterProps));
    }
  }

  public void addCluster(String clusterName, Properties clusterProps) {
    if (this.clusterPropertiesMap == null) {
      this.clusterPropertiesMap = new HashMap<>();
    }
    if (this.clusterPropertiesMap.containsKey(clusterName)) {
      System.out.println("Cluster " + clusterName + " already added to this Comparator, skipping.");
      return;
    }
    this.clusterPropertiesMap.put(clusterName, clusterProps);
    this.adminClientMap.put(clusterName, TopicTool.getAdminClient(clusterProps));
  }


  public TopicCompareResult compare(List<String> patterns, List<String> clusters) {
    if (clusters.size() != 2) {
      throw new IllegalArgumentException(
          "Currently comparing topics is only supported across two clusters!");
    }

    TopicManager manager1 = managerMap.get(clusters.get(0));
    TopicManager manager2 = managerMap.get(clusters.get(1));

    // Get all topics in scope from all clusters
    Map<String, NewTopic> topicsInScope1 = new HashMap<>();
    Map<String, NewTopic> topicsInScope2 = new HashMap<>();

    for (String pattern : patterns) {
      List<NewTopic> topics = manager1.getTopics(pattern, false);
      topicsInScope1.putAll(manager1.getTopics(pattern, false).stream().map(e -> (NewTopic) e)
          .collect(Collectors.toMap(e -> e.name(), e -> e)));
      topicsInScope2.putAll(manager2.getTopics(pattern, false).stream().map(e -> (NewTopic) e)
          .collect(Collectors.toMap(e -> e.name(), e -> e)));
    }

    // Find topics that exist in both clusters
    List<String> existingTopics = topicsInScope1.keySet().stream()
        .filter(e -> topicsInScope2.containsKey(e)).collect(Collectors.toList());

    // Filter topics by different number of partitions => can't be equal
    List<String> existingTopicsWithSamePartitionCount = existingTopics.stream()
        .filter(e -> topicsInScope1.get(e).numPartitions() == topicsInScope2.get(e).numPartitions())
        .collect(Collectors.toList());

    // Start compare thread per Partition of remaining topics
    List<Future<PartitionCompareResult>> compareFutures = new ArrayList<>();

    logger.debug(
        "Number of topics in both clusters with same partition count that will be compared: "
            + existingTopicsWithSamePartitionCount.size());
    boolean compacted = false;
    for (String topic : existingTopicsWithSamePartitionCount) {
      Map<String, String> topicConfigs = topicsInScope1.get(topic).configs();
      logger.debug("Config for topic " + topic + ": " + topicConfigs);
      if (topicConfigs == null) {
        logger.debug(
            "Got null config object for topic " + topic + ", assuming delete as cleanup policy.");
      } else {
        // .contains() is used instead of .equals() as the
        // combination "delete,compact" is also possible
        compacted = topicConfigs.get("cleanup.policy").contains("compact");
      }
      if (compacted) {
        System.out.println("Scheduling comparison for compacted topic " + topic);
      } else {
        System.out.println("Scheduling comparison for non-compacted topic " + topic);
      }
      for (int i = 0; i < topicsInScope1.get(topic).numPartitions(); i++) {
        if (compacted) {
          compareFutures.add(executor.submit(
              new CompactionCompareThread(new TopicPartition(topic, i), topicConfigs,
                  clusterPropertiesMap)));
        } else {
          compareFutures.add(executor.submit(
              new DeletionCompareThread(new TopicPartition(topic, i), topicConfigs,
                  clusterPropertiesMap)));
        }
      }
    }

    return new TopicCompareResult(compareFutures);
  }

  private boolean nullSafeEquals(Object o1, Object o2) {
    if (o1 == null && o2 == null) {
      return true;
    } else if (o1 != null && o2 != null) {
      return o1.equals(o2);
    } else {
      return false;
    }
  }

  public void close() {
    executor.shutdown();
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}



