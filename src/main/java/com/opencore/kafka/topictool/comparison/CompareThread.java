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
import com.opencore.kafka.topictool.serialization.Murmur2Deserializer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;

public class CompareThread {

  List<TopicPartition> partition;
  Map<String, String> topicConfig;
  String topicPartition;
  Map<String, Properties> clusterPropertiesMap;
  Logger logger = null;

  long compareUntilOffset1 = -1L;
  long compareUntilOffset2 = -1L;

  long lastPolledOffset1 = -1L;
  long lastPolledOffset2 = -1L;

  KafkaConsumer<String, String> consumer2 = null;
  KafkaConsumer<String, String> consumer1 = null;

  PartitionCompareResult result = null;

  public CompareThread(TopicPartition partition, Map<String, String> configs,
      Map<String, Properties> clusterPropertiesMap) {
    this.partition = Collections.singletonList(partition);
    this.topicConfig = configs;
    this.topicPartition = partition.topic() + "-" + partition.partition();
    this.clusterPropertiesMap = clusterPropertiesMap;
  }

  boolean nullSafeEquals(Object o1, Object o2) {
    if (o1 == null && o2 == null) {
      return true;
    } else if (o1 != null && o2 != null) {
      return o1.equals(o2);
    } else {
      return false;
    }
  }

  boolean compareRecords(ConsumerRecord record1, ConsumerRecord record2) {
    // TODO: make this pluggable
    return nullSafeEquals(record1.key(), record2.key())
        && nullSafeEquals(record1.value(), record2.value())
        && nullSafeEquals(record1.timestamp(), record2.timestamp());
  }

  void initialize() {
    String cluster1 = clusterPropertiesMap.keySet()
        .toArray()[0].toString();
    logger.debug("Defining " + cluster1 + " as cluster1");
    String cluster2 = clusterPropertiesMap.keySet()
        .toArray()[1].toString();
    logger.debug("Defining " + cluster2 + " as cluster2");
    // TODO: add code to check for largest offset in partition at this moment to avoid needlessly
    //  polling
    logger.debug(clusterPropertiesMap.keySet()
        .toString());

    result = new PartitionCompareResult();
    result.setTopic(partition.get(0)
        .topic());
    result.setPartition(partition.get(0)
        .partition());

    Properties consumerProps1 = clusterPropertiesMap.get(cluster1);
    consumerProps1.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        Murmur2Deserializer.class.getCanonicalName());
    consumerProps1.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getCanonicalName());
    consumerProps1.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    Properties consumerProps2 = clusterPropertiesMap.get(cluster2);
    consumerProps2.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        Murmur2Deserializer.class.getCanonicalName());
    consumerProps2.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getCanonicalName());
    consumerProps2.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    consumer1 = new KafkaConsumer(consumerProps1);
    consumer2 = new KafkaConsumer(consumerProps2);

    // Assign consumers to partitions - may fail if topic was deleted in the meantime (fringe
    // case, Exception will
    // be caught and treated when the Future is unwrapped later on
    consumer1.assign(partition);
    consumer2.assign(partition);

    // find current latest offset
    Map endOffsets1 = consumer1.endOffsets(partition);
    Map endOffsets2 = consumer2.endOffsets(partition);

    compareUntilOffset1 = (Long) endOffsets1.get(partition.get(0)) - 1;
    compareUntilOffset2 = (Long) endOffsets2.get(partition.get(0)) - 1;

    logger.debug("Setting end offsets for partition " + partition.get(0)
        .partition() + " to " + compareUntilOffset1 + "/" + compareUntilOffset2);

    // Nothing can be guessed from these offsets yet, so no additional checking performed

    // Start at beginning of topic
    logger.debug("Seeking to beginning for partition " + partition.get(0)
        .partition());
    int batchSize = 100;
    consumer1.seekToBeginning(partition);
    consumer2.seekToBeginning(partition);
  }
}
