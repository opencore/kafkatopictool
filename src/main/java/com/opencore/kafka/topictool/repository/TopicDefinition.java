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

package com.opencore.kafka.topictool.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AclBinding;

public class TopicDefinition {

  String name;
  Integer partitionCount;
  Short replicationfactor;
  List<String> clusters;
  Map<String, String> configs;
  List<AclDefinition> acls;


  public TopicDefinition(NewTopic topicToClone, List<String> clusters) {
    this.name = topicToClone.name();
    this.partitionCount = topicToClone.numPartitions();
    this.replicationfactor = topicToClone.replicationFactor();
    this.configs = topicToClone.configs();
    this.clusters = clusters;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Integer getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(Integer partitionCount) {
    this.partitionCount = partitionCount;
  }

  public Short getReplicationfactor() {
    return replicationfactor;
  }

  public void setReplicationfactor(Short replicationfactor) {
    this.replicationfactor = replicationfactor;
  }

  public List<String> getClusters() {
    return clusters;
  }

  public void setClusters(List<String> clusters) {
    this.clusters = clusters;
  }

  public Map<String, String> getConfigs() {
    return configs;
  }

  public void setConfigs(Map<String, String> configs) {
    this.configs = configs;
  }

  public List<AclDefinition> getAcls() {
    return acls;
  }

  public void setAcls(List<AclDefinition> acls) {
    this.acls = acls;
  }

  public NewTopic getNewTopic() {
    NewTopic result = new NewTopic(getName(), getPartitionCount(), getReplicationfactor());
    result.configs(getConfigs());
    return result;
  }

  public List<AclBinding> getAclBindings() {
    List<AclBinding> allAcls = new ArrayList<>();
    for (AclDefinition currentAclDef : getAcls()) {
      allAcls.addAll(currentAclDef.getAclBindings(this));
    }
    return allAcls;
  }
}
