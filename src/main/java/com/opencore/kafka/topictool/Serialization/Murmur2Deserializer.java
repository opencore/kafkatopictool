package com.opencore.kafka.topictool.Serialization;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

public class Murmur2Deserializer implements Deserializer {
  @Override
  public void configure(Map map, boolean b) {

  }

  @Override
  public Integer deserialize(String s, byte[] bytes) {
    return Utils.murmur2(bytes);
  }

  @Override
  public void close() {

  }
}
