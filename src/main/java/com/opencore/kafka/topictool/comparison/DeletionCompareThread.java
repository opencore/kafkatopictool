/**
 * Copyright © 2019 Sönke Liebau (soenke.liebau@opencore.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.opencore.kafka.topictool.comparison;

import com.opencore.kafka.topictool.PartitionCompareResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

public class DeletionCompareThread extends CompareThread implements
    Callable<PartitionCompareResult> {

  protected static final int BATCH_SIZE = 100;

  public DeletionCompareThread(TopicPartition partition, Map<String, String> configs, Map<String,
      Properties> clusters) {
    super(partition, configs, clusters);
    logger = LoggerFactory.getLogger(this.getClass()
        .getCanonicalName());
  }

  @Override
  public PartitionCompareResult call() throws Exception {
    initialize();
    List<ConsumerRecord<String, String>> topicRecords1 = new LinkedList<>();
    List<ConsumerRecord<String, String>> topicRecords2 = new LinkedList<>();

    logger.debug("Polling cluster1 " + topicPartition);
    topicRecords1.addAll(poll(consumer1));
    logger.debug("Polling cluster2 " + topicPartition);
    topicRecords2.addAll(poll(consumer2));

    int emptyPolls = 0;

    // This is the main comparison loop, logic is:
    // - Compare message by message until one of the two buffers is empty.
    // - Poll both topics
    // - Rerun comparison
    // - Break when we have seen at least five consecutive empty polls on
    //   both topics or both topics have been read beyond the
    //   largest offset we initially measured

    /*while ((lastPolledOffset1 < compareUntilOffset1 || lastPolledOffset2 < compareUntilOffset2)
        && !(topicRecords1.isEmpty() && topicRecords2.isEmpty())
        && emptyPolls < 5) { */
    while (lastPolledOffset1 < compareUntilOffset1 && lastPolledOffset2 < compareUntilOffset2
        && emptyPolls < 5) {
      while (!(topicRecords1.isEmpty() || topicRecords2.isEmpty())) {
        // Check if either of our queues is empty, if yes, skip this iteration
        // Handling of empty polls and breaking the entire comparison is handled in the
        // wrapping while loop
        logger.trace("Records in queue1: " + topicRecords1.size() + " - Records in queue2: "
            + topicRecords2.size());
        if (topicRecords1.isEmpty() || topicRecords2.isEmpty()) {
          logger.debug("Skipping this comparison, one or more of the queues is empty.");
          break;
        }

        ConsumerRecord<String, String> record1 =
            ((LinkedList<ConsumerRecord<String, String>>) topicRecords1).poll();

        ConsumerRecord<String, String> record2 =
            ((LinkedList<ConsumerRecord<String, String>>) topicRecords2).poll();
        /* Possible scenarios at this point:
        1. Both records are null: we've reached the end of both topics, but will poll again a
        couple of times, just to be sure
        2. One record is null: we've reached the end of one topic, poll again to be sure ->
        topics are the same, but one is               missing a few messages
        3. Both records are non-null: records are left in both topics, continue comparison
        TODO: factor in whether we have read past the lastCompareOffset for one or both topics
         */

        if (record1 == null || record2 == null) {
          // Shouldn't happen, as we checked for emptiness of our queues but you never know
          throw (new RuntimeException("Got null record when we did not expect one!"));
        } else if (record1 != null && record2 != null) {

          if (!compareRecords(record1, record2)) {
            logger.debug("Mismatch in records: " + record1.toString() + " - " + record2.toString());
            result.setFailedOffset1(record1.offset());
            result.setFailedOffset2(record2.offset());
            result.setResult(PartitionCompareResult.MISMATCH);
            return result;
          } else {
            logger.trace("Match for partition " + partition.get(0)
                .partition() + " - at offsets " + record1.offset() + "/" + record2.offset());
            lastPolledOffset1 = record1.offset();
            lastPolledOffset2 = record2.offset();
          }
        }
      }

      logger.debug("Polling cluster1 " + topicPartition);
      List<ConsumerRecord<String, String>> pollResult1 = poll(consumer1);
      topicRecords1.addAll(pollResult1);

      logger.debug("Polling cluster2 " + topicPartition);
      List<ConsumerRecord<String, String>> pollResult2 = poll(consumer2);
      topicRecords2.addAll(pollResult2);

      // Check if both polls were empty and keep a count
      if (pollResult2.isEmpty() && pollResult1.isEmpty()) {
        logger.debug("Empty poll on both topics..");
        emptyPolls++;
      } else {
        logger.debug("Resetting empty poll counter..");
        emptyPolls = 0;
      }
    }

    // If we made it here, all compared messages were equal
    // There are still scenarios that need to be treated differently
    // from a full on match:
    // 1. Data left in one of the two topics - one topic is not fully caught up
    // 2. One or both of the topic were read past the endoffset we looked up at
    //    the beginning - topic is still being written to

    logger.debug(
        "Sizes of queues for " + topicPartition + " after comparison loop: " + topicRecords1.size()
            + "/"
            + topicRecords2.size());
    if (lastPolledOffset1 > compareUntilOffset1 || lastPolledOffset2 > compareUntilOffset2) {
      logger.debug(topicPartition
          + " had records written to it since the comparison started, results may be incorrect.");
      result.setResult(PartitionCompareResult.ACTIVE);
    } else if (!(topicRecords1.isEmpty() && topicRecords2.isEmpty())) {
      // no topic was read past the calculated end offset, but we still have messages left
      // for one of them, so there have to be additional messages in one of the topics
      result.setResult(PartitionCompareResult.PARTIAL);
    }
    // Close consumers
    consumer1.close();
    consumer2.close();
    return result;
  }

  private List<ConsumerRecord<String, String>> poll(KafkaConsumer consumer) {
    logger.debug("Polling  " + topicPartition);
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
    logger.debug("Got " + records.count() + " records for " + topicPartition);

    List<ConsumerRecord<String, String>> result = new ArrayList<>();
    for (ConsumerRecord<String, String> record : records) {
      if (record != null) {
        result.add(record);
      }
    }
    return result;

  }
}
