package com.opencore.kafka.topictool;

public class PartitionCompareResult {
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
}

