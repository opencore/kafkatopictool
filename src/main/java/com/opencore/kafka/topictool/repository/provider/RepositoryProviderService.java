package com.opencore.kafka.topictool.repository.provider;

import com.opencore.kafka.topictool.repository.TopicDefinition;
import java.util.Map;

public interface RepositoryProviderService {
  Map<String, TopicDefinition> getTopics(String cluster);
}
