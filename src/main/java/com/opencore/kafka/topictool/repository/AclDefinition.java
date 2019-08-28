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
