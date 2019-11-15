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

package com.opencore.kafka.topictool.command;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.opencore.kafka.topictool.OffsetSetter;
import com.opencore.kafka.topictool.PartitionCompareResult;
import com.opencore.kafka.topictool.TopicCompareResult;
import com.opencore.kafka.topictool.TopicComparer;
import com.opencore.kafka.topictool.TopicManager;
import com.opencore.kafka.topictool.TopicToolConfig;
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

/**
 * A class that represents a command for the TopicTool to execute.
 * Currently implemented commands are:
 *  - sync
 *  - compare
 *  - export
 *
 *  Objects of this class should be created by various clients and
 *  passed into this lib to be executed.
 */
public abstract class TopicToolCommand {
  private String targetCluster;
  private String sourceCluster;
  private String targetFile;
  private String sourceRepository;
  private boolean simulate;

  public String getTargetFile() {
    return targetFile;
  }

  public boolean getSimulate() {
    return simulate;
  }

  public void setSimulate(boolean simulate) {
    this.simulate = simulate;
  }

  public void setTargetFile(String targetFile) {
    this.targetFile = targetFile;
  }

  public String getSourceRepository() {
    return sourceRepository;
  }

  public void setSourceRepository(String sourceRepository) {
    this.sourceRepository = sourceRepository;
  }

  public abstract int getAction();

  public abstract boolean checkCommand();

  public String getTargetCluster() {
    return targetCluster;
  }

  public void setTargetCluster(String targetCluster) {
    this.targetCluster = targetCluster;
  }

  public String getSourceCluster() {
    return sourceCluster;
  }

  public void setSourceCluster(String sourceCluster) {
    this.sourceCluster = sourceCluster;
  }

  public static void main(String[] args) {
    TopicToolConfig config = new TopicToolConfig(args);


    Map<String, TopicManager> topicManagerMap = new HashMap<>();
    for (String cluster : config.getClusterConfigs().keySet()) {
      topicManagerMap.put(cluster, new TopicManager(cluster, config.getClusterConfig(cluster)));
    }

    Map<String, Properties> repoProperties = config.getRepoConfigs();

    if (config.getConfig().getString("command") == "compare") {
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
          match = match && partitionResult.isMatch();
          if (detailed) {
            boolean partResult = partitionResult.isMatch();
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
