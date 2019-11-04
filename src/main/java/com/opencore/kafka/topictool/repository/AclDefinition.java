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
package com.opencore.kafka.topictool.repository;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

public class AclDefinition {
  List<String> principal;
  List<String> host;
  List<String> operation;
  String permissiontype;
  List<String> group;

  public List<AclBinding> getAclBindings(TopicDefinition topic) {
    List <AclBinding> result = new ArrayList<>();

    for (String currentPrincipal : principal) {
      for (String currentOperation : operation) {
        for (String currentHost : host) {
          for (String currentGroup : group) {
            AclOperation operationKafka = AclOperation.fromString(currentOperation);
            System.out.println(topic.name +": " + currentPrincipal + " - " + currentHost + " - " + operationKafka.toString() + " - " + currentOperation + " - " + currentGroup);
          }
        }
      }
    }
    return result;
  }

  public ResourcePattern getResourcePattern(TopicDefinition topicDefinition) {
    return new ResourcePattern(ResourceType.TOPIC, topicDefinition.getName(), PatternType.LITERAL);
  }

  public AccessControlEntry getAccessControlEntry(TopicDefinition topic) {
    //return new AccessControlEntry(getPrincipal(), getHost(), getOperation(), AclPermissionType.ALLOW);
    return null;
  }

  public List<String> getPrincipal() {
    return principal;
  }

  public void setPrincipal(List<String> principal) {
    this.principal = principal;
  }

  public List<String> getHost() {
    return host;
  }

  public void setHost(List<String> host) {
    this.host = host;
  }

  public List<String> getOperation() {
    return operation;
  }

  public void setOperation(List<String> operation) {
    this.operation = operation;
  }

  public String getPermissiontype() {
    return permissiontype;
  }

  public void setPermissiontype(String permissiontype) {
    this.permissiontype = permissiontype;
  }

  public List<String> getGroup() {
    return group;
  }

  public void setGroup(List<String> group) {
    this.group = group;
  }
}
