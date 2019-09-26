package com.opencore.kafka.topictool.repository.provider;

import com.opencore.kafka.topictool.TopicManager;
import com.opencore.kafka.topictool.repository.TopicDefinition;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.NewTopic;

public class KafkaRepositoryProvider implements RepositoryProviderService {
  private Properties repoProperties;
  private List<String> targetClusters = null;
  private TopicManager topicManager = null;
  private String filterRegex = null;


  public KafkaRepositoryProvider(Properties repoProperties) {
    this.repoProperties = repoProperties;
    this.topicManager = new TopicManager("kafkarepo", repoProperties);
    this.filterRegex = repoProperties.getProperty("topicfilter");
    String clustersString = repoProperties.getProperty("copytocluster");
    this.targetClusters = Arrays.asList(clustersString.split(","));
  }

  @Override
  public Map<String, TopicDefinition> getTopics(String cluster) {
    List<NewTopic> topics;
    if (filterRegex != null) {
      topics = topicManager.getTopics(filterRegex, true);
    } else {
      topics = topicManager.getTopics(true);
    }

    Map<String, TopicDefinition> topicsResult = new HashMap<>();

    for (NewTopic topic : topics) {
      topicsResult.put(topic.name(), new TopicDefinition(topic, targetClusters));
    }
    return topicsResult;
  }
}
