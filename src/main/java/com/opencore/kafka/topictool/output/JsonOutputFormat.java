package com.opencore.kafka.topictool.output;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import org.apache.kafka.clients.admin.NewTopic;

public class JsonOutputFormat implements OutputFormatService {
  public static String FORMAT_NAME = "json";

  Gson gson;

  @Override
  public String formatName() {
    return JsonOutputFormat.FORMAT_NAME;
  }

  protected Gson getGson() {
    return new GsonBuilder().create();
  }

  public JsonOutputFormat() {
      this.gson = getGson();
  }

  @Override
  public String format(List<NewTopic> topicList) {
    gson.toJson(topicList, System.out);
    return null;
  }
}
