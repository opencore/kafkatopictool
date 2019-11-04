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
package com.opencore.kafka.topictool.repository.provider;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.opencore.kafka.topictool.repository.TopicDefinition;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class LocalDirRepositoryProvider implements RepositoryProviderService {
  private static final Type TOPIC_TYPE = new TypeToken<List<TopicDefinition>>() {
  }.getType();
  private Properties repoProps;

  public LocalDirRepositoryProvider(Properties repoProps) {
    this.repoProps = repoProps;
    getTopics("");
  }

  @Override
  public Map<String, TopicDefinition> getTopics(String cluster) {
    List<TopicDefinition> topics = new ArrayList<>();
    try {
      List<Path> inputFiles = Files.walk(Paths.get(repoProps.getProperty("path")))
          .filter(Files::isRegularFile)
          .filter(file -> file.toString().endsWith(".json"))  // TODO: switch this to using a PathMatcher
          .collect(Collectors.toList());

      Gson gson = new GsonBuilder().create();
      for (Path file : inputFiles) {

        try (JsonReader reader = new JsonReader(new FileReader(file.toFile()))) {
          List<TopicDefinition> topicsFromCurrentfile = gson.fromJson(reader, TOPIC_TYPE);
          topics.addAll(topicsFromCurrentfile);
        }
      }
      System.out.println();
    } catch (IOException e) {
      e.printStackTrace();
    }

    topics = topics.stream().filter(e -> e.getClusters().contains(cluster)).collect(Collectors.toList());
    // Convert list to map with topicnames as key - if duplicates are found none of these are kept to avoid unspecified behaviour
    // TODO: add logging for duplicates
    Map <String, TopicDefinition> topicMap = topics.stream()
        .collect(Collectors.toMap(e -> e.getName(), e -> e, (existing, replacement) -> existing));
    for (TopicDefinition topic : topicMap.values()) {
      topic.getAclBindings();
    }
    return topicMap;
  }

  public Map<String, List<TopicDefinition>> getTopicsPerCluster() {
    return null;
  }
}
