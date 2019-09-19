package com.opencore.kafka.topictool;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.opencore.kafka.topictool.repository.TopicDefinition;
import com.opencore.kafka.topictool.repository.provider.KafkaRepositoryProvider;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
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
      topicManagerMap.put(cluster, new TopicManager(config.getClusterConfig(cluster)));
    }


    Map<String, Properties> repoProperties = config.getRepoProperties();
    KafkaRepositoryProvider repo = new KafkaRepositoryProvider(repoProperties.get("kafka"));

    if (config.getConfig("command") == "export") {
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
    } else if (config.getConfig("command") == "sync") {
      for (String clusterName : topicManagerMap.keySet()) {
        TopicManager topicManager = topicManagerMap.get(clusterName);

        Map <String, TopicDefinition> topicList = repo.getTopics(clusterName);
        List<NewTopic> tl = topicList.values().stream().map(e -> e.getNewTopic()).collect(Collectors.toList());
        topicManager.sync(tl);
      }

      //topicManager.sync(repo.getTopics());
    }



    System.out.println();
  }
}
