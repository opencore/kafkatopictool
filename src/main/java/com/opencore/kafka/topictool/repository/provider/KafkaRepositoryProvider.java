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

package com.opencore.kafka.topictool.repository.provider;

import com.opencore.kafka.topictool.TopicManager;
import com.opencore.kafka.topictool.repository.TopicDefinition;
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
    configure(repoProperties);
  }

  @Override
  public void configure(Properties repoProperties) {
    this.repoProperties = repoProperties;
    this.topicManager = new TopicManager("kafkarepo", repoProperties);
    this.filterRegex = repoProperties.getProperty("topicfilter");
  }

  @Override
  public String repositoryProviderType() {
    return "kafka";
  }

  public KafkaRepositoryProvider() {
    super();
  }

  @Override
  public Map<String, TopicDefinition> getTopics(String topicFilter) {
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
