package com.opencore.kafka.topictool.comparison;

import com.opencore.kafka.topictool.PartitionCompareResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

public class DeletionCompareThread extends CompareThread implements Callable<PartitionCompareResult> {
    protected final int BATCHSIZE = 100;
    public DeletionCompareThread(TopicPartition partition, Map<String, String> configs, Map<String, Properties> clusters) {
        super(partition, configs, clusters);
        logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());
    }

    @Override
    public PartitionCompareResult call() throws Exception {
        initialize();
        List<ConsumerRecord<String, String>> topicRecords1 = new LinkedList<>();
        List<ConsumerRecord<String, String>> topicRecords2 = new LinkedList<>();

        int emptyPolls1 = 0;
        int emptyPolls2 = 0;

        while ((lastPolledOffset1 < compareUntilOffset1 || lastPolledOffset2 < compareUntilOffset2) && (!topicRecords1.isEmpty() || topicRecords2.isEmpty() )) {
            logger.debug("Polling cluster1 " + topicPartition);
            ConsumerRecords<String, String> records1 = consumer1.poll(Duration.ofSeconds(3));
            logger.debug("Got " + records1.count() + " records for " + topicPartition);

            for (ConsumerRecord<String, String> record : records1) {
                if (record != null) {
                    topicRecords1.add(record);
                }
            }

            logger.debug("Polling cluster2 " + topicPartition);
            ConsumerRecords<String, String> records2 = consumer2.poll(Duration.ofSeconds(3));
            logger.debug("Got " + records2.count() + " records for " + topicPartition);

            for (ConsumerRecord<String, String> record : records2) {
                if (record != null) {
                    topicRecords2.add(record);
                }
            }


            /*if (!checkAndPollIfNecessary(consumer1, topicRecords1, BATCHSIZE * 2)) {
                emptyPolls1++;
            } else {
                logger.trace("Resetting emptyPollCount for cluster1.");
                emptyPolls1 = 0;
            }

            if (!(checkAndPollIfNecessary(consumer2, topicRecords2, BATCHSIZE * 2))) {
                emptyPolls2++;
            } else {
                logger.trace("Resetting emptyPollCount for cluster1.");
                emptyPolls2 = 0;
            }
            // We should now have at least _batchSize_ records in our queues, if not we
            // probably read to the end of our topic

            if (emptyPolls1 > 10 && emptyPolls2 > 10 && topicRecords1.isEmpty() && topicRecords2.isEmpty()) {
                logger.debug("Aborting due to too many empty polls on partition " + partition.get(0).partition());
                // We don't have any records left in one of the topics and have polled unsuccessfully three times
                // so we can safely abort processing
                result.setFailedOffset1(lastPolledOffset1);
                result.setFailedOffset2(lastPolledOffset2);
                result.setResult(false);
                break;
            }

 */

            for (int i = 1; i <= BATCHSIZE; i++) {
                // Check if either of our queues is empty, if yes, skip this iteration
                // Handling of empty polls and breaking the entire comparison is handled in the
                // wrapping while loop
                logger.trace("Records in queue1: " + topicRecords1.size() + " - Records in queue2: " + topicRecords2.size());
                if (topicRecords1.isEmpty() || topicRecords2.isEmpty()) {
                    logger.debug("Skipping this comparison, one or more of the queues is empty.");
                    break;
                }

                ConsumerRecord<String, String> record1 = ((LinkedList<ConsumerRecord<String, String>>) topicRecords1).poll();
                ConsumerRecord<String, String> record2 = ((LinkedList<ConsumerRecord<String, String>>) topicRecords2).poll();
              /* Possible scenarios at this point:
              1. Both records are null: we've reached the end of both topics, but will poll again a couple of times, just to be sure
              2. One record is null: we've reached the end of one topic, poll again to be sure -> topics are the same, but one is missing a few messages
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
                        result.setResult(false);
                        return result;
                    } else {
                        logger.trace("Match for partition " + partition.get(0).partition() + " - at offsets " + record1.offset() + "/" + record2.offset());
                        lastPolledOffset1 = record1.offset();
                        lastPolledOffset2 = record2.offset();
                    }
                }
            }
        }

        logger.debug("Sizes of queues for " + topicPartition + ": " + topicRecords1.size() + "/" + topicRecords2.size());

        if (!result.getResult()) {
            // There was a failed record in this batch, we can stop processing

            // Close consumers
            consumer1.close();
            consumer2.close();

            // return
            return result;
        }
        // Close consumers
        consumer1.close();
        consumer2.close();
        return result;
    }

    private boolean checkAndPollIfNecessary(KafkaConsumer consumer, List queue, int limit) {
        if (queue.size() < limit) {
            ConsumerRecords<String, String> polledRecords = consumer.poll(Duration.ofSeconds(3));
            if (polledRecords.count() > 0) {
                for (ConsumerRecord<String, String> record : polledRecords) {
                    if (record != null) {
                        queue.add(record);
                    }
                }
                return true;
            } else {
                return false;
            }
        }
        // There are records left in the queue, so no need to count this as an empty poll, since we didn't poll
        return true;
    }
}