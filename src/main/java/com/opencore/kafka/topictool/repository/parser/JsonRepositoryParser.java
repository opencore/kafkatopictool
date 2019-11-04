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
