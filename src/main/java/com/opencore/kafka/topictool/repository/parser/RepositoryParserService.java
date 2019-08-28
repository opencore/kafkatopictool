package com.opencore.kafka.topictool.repository.parser;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;

public interface RepositoryParserService {
  String formatName();

  Map<String, List<NewTopic>> getTopics(String cluster);


}
