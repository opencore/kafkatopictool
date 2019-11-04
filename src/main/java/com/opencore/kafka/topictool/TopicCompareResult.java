/**
 * Copyright © 2019 Sönke Liebau (soenke.liebau@opencore.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

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
