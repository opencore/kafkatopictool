/**
 * Copyright © 2019 Sönke Liebau (soenke.liebau@opencore.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.opencore.kafka.topictool;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.opencore.kafka.topictool.repository.TopicDefinition;
import com.opencore.kafka.topictool.repository.provider.KafkaRepositoryProvider;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;

public class TopicToolCommand {

  public static void main(String[] args) {
    TopicToolConfig config = new TopicToolConfig(args);

    Map<String, TopicManager> topicManagerMap = new HashMap<>();
    for (String cluster : config.getClusterConfigs().keySet()) {
      topicManagerMap.put(cluster, new TopicManager(cluster, config.getClusterConfig(cluster)));
    }

    Map<String, Properties> repoProperties = config.getRepoProperties();

    if (config.getConfig().getString("command") == "export") {
      List<String> cluster = config.getList("cluster");
      if (cluster == null) {
        System.out.println("Exporting topics from all clusters!");

      } else {
        System.out.println("Exporting topics from clusters " + cluster.toString());
      }
      Gson gson = new GsonBuilder().create();

      List<String> inScopeClusters = config.getList("cluster");
      for (String clusterName : topicManagerMap.keySet()) {
        if (inScopeClusters.contains(clusterName)) {
          TopicManager clusterManager = topicManagerMap.get(clusterName);
          List<TopicDefinition> topicsToExport = clusterManager.getTopics()
              .stream()
              .map(e -> new TopicDefinition(e, null))
              .collect(Collectors.toList());

          try (Writer writer = new FileWriter(clusterName + ".json")) {
            gson.toJson(topicsToExport, writer);
          } catch (IOException e) {
            System.out.println(e.getMessage());
          }
        }
      }

      System.exit(0);
    } else if (config.getConfig().getString("command") == "sync") {
      KafkaRepositoryProvider repo = new KafkaRepositoryProvider(repoProperties.get("kafka"));
      for (String clusterName : topicManagerMap.keySet()) {
        TopicManager topicManager = topicManagerMap.get(clusterName);

        Map<String, TopicDefinition> topicList = repo.getTopics(clusterName);
        List<NewTopic> tl = topicList.values().stream().map(e -> e.getNewTopic())
            .collect(Collectors.toList());
        topicManager.sync(tl, config.getConfig().getBoolean("simulate"));
      }

      //topicManager.sync(repo.getTopics());
    } else if (config.getConfig().getString("command") == "compare") {
      boolean printMismatchOnly = config.getConfig().getBoolean("mismatchonly");
      boolean detailed = config.getConfig().getBoolean("detailed");

      List<String> topics = config.getList("topics");
      TopicComparer comparer = new TopicComparer(config.getClusterConfigs(),
          config.getConfig().getInt("threadcount"));

      List<String> clusterList = new ArrayList<>();
      clusterList.addAll(config.getClusterConfigs().keySet());
      TopicCompareResult result = comparer.compare(topics, clusterList);

      Map<String, List<PartitionCompareResult>> resultMap = result.all();
      System.out.println("Compared " + resultMap.size() + " topics..");
      for (String topic : resultMap.keySet().stream().sorted().collect(Collectors.toList())) {
        boolean match = true;
        for (PartitionCompareResult partitionResult : resultMap.get(topic)) {
          match = match && partitionResult.getResult();
          if (detailed) {
            boolean partResult = partitionResult.getResult();
            StringBuilder resultString = new StringBuilder();
            System.out.println(
                "Partition " + partitionResult.getPartition() + ": " + partitionResult.toString());
          }
        }
        if (!printMismatchOnly || (!match && printMismatchOnly)) {
          System.out.println(topic + ": " + (match ? "MATCH" : "MISMATCH"));
        }

      }
      comparer.close();
    } else if (config.getConfig().getString("command") == "setoffsets") {
      OffsetSetter offsetSetter = new OffsetSetter(config.getClusterConfigs());
      File offsetsFile = config.getConfig().get("offsetsfile");
      List<String> clusters = config.getList("cluster");
      List<String> groups = config.getList("groups");
      List<String> topics = config.getList("topics");
      boolean useDate = config.getConfig().getBoolean("usedate");
      offsetSetter.setOffsets(offsetsFile, clusters, useDate);
    }
  }
}
