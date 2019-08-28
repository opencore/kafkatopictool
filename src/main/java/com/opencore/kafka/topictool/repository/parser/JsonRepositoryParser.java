package com.opencore.kafka.topictool.repository.parser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;

public class JsonRepositoryParser implements RepositoryParserService {

  private File configFile = null;
  private List<NewTopic> topicList = null;
  Gson gson = new GsonBuilder().create();

  @Override
  public String formatName() {
    return null;
  }

  @Override
  public Map<String, List<NewTopic>> getTopics(String cluster) {
    return null;
  }
}
