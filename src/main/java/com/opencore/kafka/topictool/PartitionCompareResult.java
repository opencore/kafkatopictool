package com.opencore.kafka.topictool;

import org.apache.kafka.common.protocol.types.Field;

public class PartitionCompareResult {
  String topic;
  int partition;

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }

  boolean result = true;
  Long failedOffset1 = -1L;
  Long failedOffset2 = -1L;

  public boolean getResult() {
    return result;
  }

  public void setResult(boolean result) {
    this.result = result;
  }

  public Long getFailedOffset1() {
    return failedOffset1;
  }

  public void setFailedOffset1(Long failedOffset1) {
    this.failedOffset1 = failedOffset1;
  }

  public Long getFailedOffset2() {
    return failedOffset2;
  }

  public void setFailedOffset2(Long failedOffset2) {
    this.failedOffset2 = failedOffset2;
  }

  public String toString() {
    StringBuilder resultString = new StringBuilder();
    resultString.append(result ? "MATCH" : "MISMATCH");
    if (!result) {
      resultString.append(" - Mismatch at offsets " + failedOffset1 + "/" + failedOffset2);
    }
    return resultString.toString();
  }
}

