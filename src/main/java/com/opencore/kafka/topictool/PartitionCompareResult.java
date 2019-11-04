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
package com.opencore.kafka.topictool;

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

