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
package com.opencore.kafka.topictool.comparison;

import com.opencore.kafka.topictool.PartitionCompareResult;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

public class CompactionCompareThread extends CompareThread
    implements Callable<PartitionCompareResult> {

  public CompactionCompareThread(TopicPartition partition, Map<String, String> configs,
      Map<String, Properties> clusters) {
    super(partition, configs, clusters);
    logger = LoggerFactory.getLogger(this.getClass()
        .getCanonicalName());
  }

  @Override
  public PartitionCompareResult call() throws Exception {
    initialize();
    logger.debug("Skipping _per message_ comparison for " + topicPartition + " due to compacted "
        + "topic.");

    Map<String, ConsumerRecord<String, String>> keyRecordMap1 = new HashMap<>();
    Map<String, ConsumerRecord<String, String>> keyRecordMap2 = new HashMap<>();

    while (lastPolledOffset1 < compareUntilOffset1 || lastPolledOffset2 < compareUntilOffset2) {
      logger.debug(lastPolledOffset1 + "/" + compareUntilOffset1 + " - " + lastPolledOffset2 + "/"
          + compareUntilOffset2);
      logger.debug("Polling cluster1 " + topicPartition);
      ConsumerRecords<String, String> records1 = consumer1.poll(Duration.ofSeconds(3));
      logger.debug("Got " + records1.count() + " records for " + topicPartition);
      for (ConsumerRecord<String, String> record : records1) {
        keyRecordMap1.put(record.key(), record);
        lastPolledOffset1 = record.offset();
      }

      logger.debug("Polling cluster2 " + topicPartition);
      ConsumerRecords<String, String> records2 = consumer2.poll(Duration.ofSeconds(3));
      logger.debug("Got " + records2.count() + " records for " + topicPartition);
      for (ConsumerRecord<String, String> record : records2) {
        keyRecordMap2.put(record.key(), record);
        lastPolledOffset2 = record.offset();
      }
    }
    logger.debug("Finished polling for " + topicPartition + " at offsets " + lastPolledOffset1 + "/"
        + lastPolledOffset2);
    logger.debug("Comparing final records for compacted topic: " + topicPartition);
    if (keyRecordMap1.keySet()
        .size() != keyRecordMap2.keySet()
        .size()) {
      logger.debug("Mismatch in number of keys: " + keyRecordMap1.keySet()
          .size() + "/" + keyRecordMap2.keySet()
          .size());
      logger.trace("Keyset for cluster1: " + keyRecordMap1.keySet()
          .toString());
      logger.trace("Keyset for cluster2: " + keyRecordMap2.keySet()
          .toString());
      result.setResult(PartitionCompareResult.MISMATCH);
    } else {
      logger.debug("Number of keys match, comparing values for " + topicPartition);
      for (String key : keyRecordMap1.keySet()) {
        if (!nullSafeEquals(keyRecordMap1.get(key), keyRecordMap2.get(key))) {
          result.setResult(PartitionCompareResult.MISMATCH);
          break;
        }
      }
      result.setResult(PartitionCompareResult.MATCH);
    }
    return result;
  }
}
