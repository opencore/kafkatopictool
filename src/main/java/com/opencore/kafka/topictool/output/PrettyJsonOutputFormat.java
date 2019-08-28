package com.opencore.kafka.topictool.output;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class PrettyJsonOutputFormat extends JsonOutputFormat{
  @Override
  public String formatName() {
    return PrettyJsonOutputFormat.FORMAT_NAME;
  }

  public static String FORMAT_NAME = "prettyjson";

  @Override
  protected Gson getGson() {
    return new GsonBuilder().setPrettyPrinting().create();
  }
}
