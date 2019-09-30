package com.opencore.kafka.topictool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TopicCompareResult {
  private List<Future<PartitionCompareResult>> operations;

  public TopicCompareResult(List<Future<PartitionCompareResult>> operations) {
    this.operations = operations;
  }

  public Map<String, List<PartitionCompareResult>> all() {
    Map<String, List<PartitionCompareResult>> result = new HashMap<>();
    for (Future<PartitionCompareResult> partitionFuture : operations) {
      try {
        PartitionCompareResult partitionResult = partitionFuture.get();
        if (!(result.containsKey(partitionResult.topic))) {
          result.put(partitionResult.getTopic(), new ArrayList<PartitionCompareResult>());
        }
        result.get(partitionResult.getTopic()).add(partitionResult);
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    return result;
  }
}
