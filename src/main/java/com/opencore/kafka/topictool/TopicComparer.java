package com.opencore.kafka.topictool;

import com.opencore.kafka.topictool.Serialization.Murmur2Deserializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class TopicComparer {
  Map<String, Properties> clusterPropertiesMap;
  Map<String, KafkaConsumer<Byte[], Byte[]>> consumerMap;
  Map<String, TopicManager> managerMap;
  Map<String, AdminClient> adminClientMap;
  ExecutorService executor = null;

  public TopicComparer(Map<String, Properties> clusterPropertiesMap, int threadCount) {
    this.clusterPropertiesMap = clusterPropertiesMap;
    this.managerMap = new HashMap<>();
    this.adminClientMap = new HashMap<>();
    this.executor = Executors.newFixedThreadPool(threadCount);


    for (String clusterName : clusterPropertiesMap.keySet()) {
      Properties clusterProps = clusterPropertiesMap.get(clusterName);
      managerMap.put(clusterName, new TopicManager(clusterName, clusterProps));
      adminClientMap.put(clusterName, AdminClient.create(clusterProps));
    }
  }

  public void addCluster(String clusterName, Properties clusterProps) {
    if (this.clusterPropertiesMap == null) {
      this.clusterPropertiesMap = new HashMap<>();
    }
    if (this.clusterPropertiesMap.containsKey(clusterName)) {
      System.out.println("Cluster " + clusterName + " already added to this Comparator, skipping.");
      return;
    }
    this.clusterPropertiesMap.put(clusterName, clusterProps);
    this.adminClientMap.put(clusterName, AdminClient.create(clusterProps));
  }


  public TopicCompareResult compare(List<String> patterns, List<String> clusters) {
    if (clusters.size() != 2) {
      throw new IllegalArgumentException("Currently comparing topics is only supported across two clusters!");
    }

    TopicManager manager1 = managerMap.get(clusters.get(0));
    TopicManager manager2 = managerMap.get(clusters.get(1));

    // Get all topics in scope from all clusters
    Map<String, NewTopic> topicsInScope1 = new HashMap<>();
    Map<String, NewTopic> topicsInScope2 = new HashMap<>();

    for (String pattern : patterns) {
      List<NewTopic> topics = manager1.getTopics(pattern);
      topicsInScope1.putAll(manager1.getTopics(pattern).stream().map(e -> (NewTopic) e).collect(Collectors.toMap(e -> e.name(), e -> e)));
      topicsInScope2.putAll(manager2.getTopics(pattern).stream().map(e -> (NewTopic) e).collect(Collectors.toMap(e -> e.name(), e -> e)));
    }

    // Find topics that exist in both clusters
    List<String> existingTopics = topicsInScope1.keySet().stream().filter(e -> topicsInScope2.containsKey(e)).collect(Collectors.toList());

    // Filter topics by different number of partitions => can't be equal
    List<String> existingTopicsWithSamePartitionCount = existingTopics.stream().filter(e -> topicsInScope1.get(e).numPartitions() == topicsInScope2.get(e).numPartitions()).collect(Collectors.toList());

    // Start compare thread per Partition of remaining topics
    List<Future<PartitionCompareResult>> compareFutures = new ArrayList<>();

    for (String topic : existingTopicsWithSamePartitionCount) {
      for (int i = 0; i < topicsInScope1.get(topic).numPartitions(); i++) {
        compareFutures.add(executor.submit(new PartitionCompareThread(new TopicPartition(topic, i), clusters)));
      }
    }

    // Comparison is running at this point, now we can add _dummy_ Futures for the topics that were
    // excluded due to partition count or that didn't exist in both clusters

    // TODO: add dummy Futures

    return new TopicCompareResult(compareFutures);
  }

  private class PartitionCompareThread implements Callable<PartitionCompareResult> {
    private List<TopicPartition> partition;
    private List<String> clusters;
    private boolean success = true;

    public PartitionCompareThread(TopicPartition partition, List<String> clusters) {
      this.partition = Collections.singletonList(partition);
      this.clusters = clusters;
    }

    @Override
    public PartitionCompareResult call() throws Exception {
      // TODO: add code to check for largest offset in partition at this moment to avoid needlessly polling
      Properties consumerProps1 = clusterPropertiesMap.get(clusters.get(0));
      Properties consumerProps2 = clusterPropertiesMap.get(clusters.get(1));

      PartitionCompareResult result = new PartitionCompareResult();
      result.setTopic(partition.get(0).topic());
      result.setPartition(partition.get(0).partition());

      consumerProps1.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Murmur2Deserializer.class.getCanonicalName());
      consumerProps1.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Murmur2Deserializer.class.getCanonicalName());

      consumerProps2.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Murmur2Deserializer.class.getCanonicalName());
      consumerProps2.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Murmur2Deserializer.class.getCanonicalName());

      KafkaConsumer<String, String> consumer1 = new KafkaConsumer(consumerProps1);
      KafkaConsumer<String, String> consumer2 = new KafkaConsumer(consumerProps2);

      // Assign consumers to partitions - may fail if topic was deleted in the meantime (fringe case, Exception will
      // be caught and treated when the Future is unwrapped later on
      consumer1.assign(partition);
      consumer2.assign(partition);

      // find current latest offset
      Map endOffsets1 = consumer1.endOffsets(partition);
      Map endOffsets2 = consumer2.endOffsets(partition);

      Long compareUntilOffset1 = (Long) endOffsets1.get(partition.get(0)) - 1;
      Long compareUntilOffset2 = (Long) endOffsets2.get(partition.get(0)) - 1;

      // Nothing can be guessed from these offsets yet, so no additional checking performed

      // Start at beginning of topic
      int batchSize = 100;
      consumer1.seekToBeginning(partition);
      consumer2.seekToBeginning(partition);
      Long lastComparedOffset1 = 0L;
      Long lastComparedOffset2 = 0L;
      List<ConsumerRecord<String, String>> topicRecords1 = new LinkedList<>();
      List<ConsumerRecord<String, String>> topicRecords2 = new LinkedList<>();

      int emptyPolls1 = 0;
      int emptyPolls2 = 0;
      while (lastComparedOffset1 < compareUntilOffset1 && lastComparedOffset2 < compareUntilOffset2) {
        if (!checkAndPollIfNecessary(consumer1, topicRecords1, batchSize * 2)) {
          emptyPolls1++;
        }

        if (!(checkAndPollIfNecessary(consumer2, topicRecords2, batchSize * 2))) {
          emptyPolls2++;
        }

        // We should now have at least _batchSize_ records in our queues, if not we
        // probably read to the end of our topic

        if ((emptyPolls1 > 3 && topicRecords1.isEmpty()) || (emptyPolls2 > 3 && topicRecords2.isEmpty())) {
          // We don't have any records left in one of the topics and have polled unsuccessfully three times
          // so we can safely abort processing
          result.setFailedOffset1(lastComparedOffset1);
          result.setFailedOffset2(lastComparedOffset2);
          result.setResult(false);
          break;
        }


        for (int i = 1; i <= batchSize; i++) {
          // Check if either of our queues is empty, if yes, skip this iteration
          // Handling of empty polls and breaking the entire comparison is handled in the
          // wrapping while loop
          if (topicRecords1.isEmpty() || topicRecords2.isEmpty()) {
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
            throw(new RuntimeException("Got null record when we did not expect one!"));
          } else if (record1 != null && record2 != null) {
            if (!compareRecords(record1, record2)) {
              result.setFailedOffset1(record1.offset());
              result.setFailedOffset2(record2.offset());
              result.setResult(false);
              break;
            } else {
              lastComparedOffset1 = record1.offset();
              lastComparedOffset2 = record2.offset();
            }
          }
        }
      }

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
      ConsumerRecords<String, String> polledRecords = consumer.poll(Duration.ofMillis(100));
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

  private boolean compareRecords(ConsumerRecord record1, ConsumerRecord record2) {
    // TODO: make this pluggable
    return nullSafeEquals(record1.key(), record2.key()) &&
        nullSafeEquals(record1.value(), record2.value()) &&
        nullSafeEquals(record1.timestamp(), record2.timestamp());
  }
}

  private boolean nullSafeEquals(Object o1, Object o2) {
    if (o1 == null && o2 == null) {
      return true;
    } else if (o1 != null && o2 != null) {
      return o1.equals(o2);
    } else {
      return false;
    }
  }

  public void close() {
    executor.shutdown();
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}



