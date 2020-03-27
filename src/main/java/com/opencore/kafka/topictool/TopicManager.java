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
package com.opencore.kafka.topictool;

import com.opencore.kafka.topictool.output.OutputFormatService;
import com.opencore.kafka.topictool.repository.TopicDefinition;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicManager implements AutoCloseable {

  private String clusterName;
  private Logger logger = LoggerFactory.getLogger(TopicManager.class);
  private Properties adminClientProperties;
  private AdminClient adminClient;
  private ReplicationFactorManager replicationManager;
  private String ignoreTopics;
  private boolean deleteTopics = false;

  private Map<String, OutputFormatService> availableFormatters;

  public TopicManager(String clusterName, Properties adminClientProperties) {
    this.clusterName = clusterName;
    this.adminClientProperties = adminClientProperties;
    adminClient = TopicTool.getAdminClient(adminClientProperties);

    this.replicationManager = new ReplicationFactorManager(adminClientProperties);
    ServiceLoader<OutputFormatService> loader = ServiceLoader.load(OutputFormatService.class);
    Iterator<OutputFormatService> serviceIterator = loader.iterator();
    if (adminClientProperties.containsKey("ignore.topics")) {
      ignoreTopics = adminClientProperties.getProperty("ignore.topics");
    } else {
      ignoreTopics = ".*";
    }
    availableFormatters = new HashMap<>();
    while (serviceIterator.hasNext()) {
      OutputFormatService currentService = serviceIterator.next();
      availableFormatters.put(currentService.formatName(), currentService);
    }
  }


  public TopicDescription getPartitionsForTopic(String topic) {
    Map<String, TopicDescription> topicPartitions = null;
    try {
      topicPartitions = adminClient.describeTopics(Collections.singletonList(topic)).all().get();
    } catch (InterruptedException | ExecutionException e) {
      System.out.println("Unable to describe topics on the cluster, aborting: " + e.getMessage());
      System.exit(3);
    }
    return topicPartitions.get(topic);
  }

  public List<NewTopic> getTopics() {
    return getTopics(".*", true);
  }

  public List<NewTopic> getTopics(Boolean onlyNonDefaults) {
    return getTopics(".*", onlyNonDefaults);
  }

  public List<NewTopic> getTopics(String filterRegex) {
    return getTopics(filterRegex, true);
  }

  public List<NewTopic> getTopics(String filterRegex, Boolean onlyNonDefaults) {
    List<NewTopic> newTopicList = null;
    try {
      // Retrieve list all topic names
      Set<String> topicNames = adminClient.listTopics().names().get()
          .stream()
          .filter(e -> e.matches(filterRegex)) // filter by regex
          .collect(Collectors.toSet());

      // Describe all topics from list of names
      logger.debug("Retrieving list of topics from cluster " + clusterName);
      Map<String, TopicDescription> all = adminClient.describeTopics(topicNames).all().get();
      logger.trace("Got response from cluster " + clusterName);

      // Retrieve config for all topics
      List<ConfigResource> configResourceList = all.keySet()
          .stream()
          .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
          .collect(Collectors.toList());
      logger.debug("Retrieving configs for these topics from cluster " + clusterName + ": "
          + configResourceList.toString());

      Map<ConfigResource, Config> map = adminClient.describeConfigs(configResourceList).all().get();
      logger.trace("Got response from cluster " + clusterName);

      // Convert to Map<String, Config> for ease of access later on
      Map<String, Config> configAsStringMap = map.keySet()
          .stream()
          .collect(Collectors.toMap(e -> e.name(), e -> map.get(e)));

      // Create list of NewTopic objects that will be used for json output
      newTopicList = all.values().stream()
          .map(topicDescription ->
              new NewTopic(topicDescription.name(),
                  topicDescription.partitions().size(),
                  (short) topicDescription.partitions().get(0).replicas().size()))
          .collect(Collectors.toList());

      // Get broker config for comparison with default values
      Config configResourceConfigMap = null;
      Map<String, String> configEntries = null;
      try {
        logger.debug("Retrieving list of live nodes from cluster " + clusterName);
        Collection<Node> nodeList = adminClient.describeCluster().nodes().get();
        logger.trace("Got response from cluster " + clusterName);
        List<String> list = nodeList
            .stream()
            .map(e -> e.idString())
            .collect(Collectors.toList());
        logger.debug("Got following nodes for cluster " + clusterName + ": " + nodeList.toString());

        ConfigResource firstBroker = new ConfigResource(ConfigResource.Type.BROKER, list.get(0));

        Map<ConfigResource, Config> configResourceConfigMap1 = adminClient
            .describeConfigs(Collections.singletonList(firstBroker)).all().get();
        configEntries = configResourceConfigMap1.get(firstBroker).entries()
            .stream()
            .filter(e -> e.value() != null)
            .collect(Collectors.toMap(e -> e.name(), e -> e.value()));
      } catch (Exception e) {
        e.printStackTrace();
      }
      final Map<String, String> finalConfigEntries = configEntries;
      // Determine non-default configuration for every topic and add these to the NewTopic object
      for (NewTopic topic : newTopicList) {
        // Retrieve configuration for this topic and filter out any that are set to the default value
        Config topicConfig = configAsStringMap.get(topic.name());
        List<ConfigEntry> collect = topicConfig.entries()
            .stream()
            .filter(e -> finalConfigEntries.containsKey("log." + e.name()))
            .collect(Collectors.toList());


        /* Map<String, String> filteredConfigs = topicConfig.entries().stream()
              .filter(entry -> !(entry.isDefault() && false))
              .collect(Collectors.toMap(e -> e.name(), e -> e.value()));
        */
        Map<String, String> filteredConfigs = topicConfig.entries().stream()
            .filter(entry -> (!(entry.isDefault()) || !onlyNonDefaults))
            .collect(Collectors.toMap(e -> e.name(), e -> e.value()));
        topic.configs(filteredConfigs);

      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return newTopicList;
  }

  /*
  public String getTopicsFormatted(String format) {
    return getFormatter(format).format(getTopics());
  }

  public String getTopicsFormatted(String format, Boolean onlyNonDefaults) {
    return getFormatter(format).format(getTopics(onlyNonDefaults));
  }

  public String getTopicsFormatted(String format, String filterRegex) {
    return getFormatter(format).format(getTopics(filterRegex));
  }

  public String getTopicsFormatted(String format, String filterRegex, Boolean onlyNonDefaults) {
    return getFormatter(format).format(getTopics(filterRegex, onlyNonDefaults));
  }

  private OutputFormatService getFormatter(String format) {
    return availableFormatters.get(format);
  }
  */
  public Set<String> getAvailableOutputFormats() {
    return availableFormatters.keySet();
  }

  public void sync(Map<String, TopicDefinition> topicList, boolean simulate, boolean deleteTopics) {
    // Convert to list in the format that TopicManager expects
    List<NewTopic> tl = topicList.values()
        .stream()
        .map(e -> e.getNewTopic())
        .collect(Collectors.toList());

    sync(tl, simulate, deleteTopics);
  }

  public void sync(List<NewTopic> targetTopicList, boolean simulate, boolean deleteTopics) {
    Map<String, NewTopic> targetTopicMap = targetTopicList
        .stream()
        .map(e -> (NewTopic) e)
        .collect(Collectors.toMap(e -> e.name(), e -> e));

    List<NewTopic> currentTopicList = getTopics(true);
    Map<String, NewTopic> currentTopicMap = currentTopicList
        .stream()
        .map(e -> (NewTopic) e)
        .collect(Collectors.toMap(e -> e.name(), e -> e));

    // Filter existing topics out of target state, result of this operation are topics that need to be created
    Set<NewTopic> topicsToCreate = targetTopicMap.keySet()
        .stream()
        .filter(e -> !currentTopicMap.containsKey(e))
        .map(e -> targetTopicMap.get(e))
        .collect(Collectors.toSet());

    System.out.println("\n=============================\n");
    System.out.println("Creating: ");
    for (NewTopic topic : topicsToCreate) {
      System.out
          .println(topic.name() + " - partitions: " + topic.numPartitions() + " - Replication: "
              + topic.replicationFactor() + " Configs: " + topic.configs().toString());
    }

    // Filter existing topics out of target state, and remove ignored topics, result of this operation are topics that need to be deleted
    Set<String> topicsToDelete = currentTopicList
        .stream()
        .filter(e -> !targetTopicMap.containsKey(e.name()))
        .filter(e -> !e.name().matches(ignoreTopics))
        .map(e -> e.name())
        .collect(Collectors.toSet());
    // TODO: identify and remove topics already marked for deletion

    System.out.println("\n=============================\n");
    System.out.println("Deleting: ");
    for (String topic : topicsToDelete) {
      System.out.println(topic);
    }

    // Filter topics that exist in current and target state and are not equal (by our definition) -> need to be modified
    Set<String> topicsToModify = currentTopicMap.keySet()
        .stream()
        .filter(e -> targetTopicMap
            .containsKey(e)) // topic names that exist in both the target and current state
        .filter(e -> !compareTopics(currentTopicMap.get(e), targetTopicMap.get(e)))
        .collect(Collectors.toSet());

    System.out.println("\n=============================\n");
    System.out.println("Modifying: ");
    // Identify changes that are necessary changes for all topics to be modified
    Map<String, NewPartitions> operations = new HashMap<>();
    Map<ConfigResource, Config> configChangeOperations = new HashMap<>();

    for (String topicToModify : topicsToModify) {
      NewTopic currentState = currentTopicMap.get(topicToModify);
      NewTopic targetState = targetTopicMap.get(topicToModify);

      // Check if configuration differs
      Map<String, String> currentConfigs = currentState.configs();
      Map<String, String> targetConfigs = targetState.configs();

      if (!compareTopicConfig(currentConfigs, targetConfigs)) {
        // Configs are different, create changeset
        List<String> nonMatchingSettings = targetConfigs.keySet().stream()
            .filter(setting -> !targetConfigs.get(setting).equals(currentConfigs.get(setting)))
            .collect(Collectors.toList());
        for (String setting : nonMatchingSettings) {
          System.out.println(
              "Topic " + topicToModify +" - changing setting " + setting + " from " + currentConfigs.get(setting) + " to "
                  + targetConfigs.get(setting));
        }

        List<String> removedSettings =
            currentConfigs.keySet().stream().filter(setting -> !targetConfigs.containsKey(setting))
                .collect(Collectors.toList());
        for (String setting : removedSettings) {
          System.out.println("Topic " + topicToModify +" - removing setting " + setting);
        }
      }

      ConfigResource topic = new ConfigResource(Type.TOPIC, topicToModify);
      List<ConfigEntry> targetConfiguration = targetConfigs.keySet().stream()
          .map(setting -> new ConfigEntry(setting, targetConfigs.get(setting))).collect(
              Collectors.toList());
      configChangeOperations.put(topic, new Config(targetConfiguration));

      // Check if partition count is different
      if (currentState.numPartitions() > targetState.numPartitions()) {
        System.out.println(
            "Error: cannot reduce number of partitions for topic " + topicToModify + " from "
                + currentState.numPartitions() + " to " + targetState.numPartitions()
                + " - skipping this change!");
      } else if (currentState.numPartitions() < targetState.numPartitions()) {
        //System.out.println("Queuing operation to add " + (targetState.numPartitions() - currentState.numPartitions()) + " partitions for topic " + topicToModify);
        System.out.println(
            "Topic " + topicToModify + ": increasing partition count from " + currentState.numPartitions()
                + " to " + targetState.numPartitions());
        operations.put(topicToModify, NewPartitions.increaseTo(targetState.numPartitions()));
      }

      // Check if replication factor is different
      if (currentTopicMap.get(topicToModify).replicationFactor() != targetTopicMap
          .get(topicToModify).replicationFactor()) {
        //System.out.println("Changing replication factor for topic " + topicToModify);
        System.out.println(topicToModify + ": changing replication factor from " + currentTopicMap
            .get(topicToModify).replicationFactor() + " to " + targetTopicMap.get(topicToModify)
            .replicationFactor());
        try {
          Map<String, TopicDescription> map = adminClient
              .describeTopics(Collections.singletonList(topicToModify)).all().get();
          replicationManager.addOperation(map.get(topicToModify),
              targetTopicMap.get(topicToModify).replicationFactor());
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
    }

    // Execute operations
    int failedOperations = 0;
    if (!simulate) {
      System.out.println("Executing actions!");
      try {
        adminClient.createTopics(topicsToCreate).all().get();
        if (deleteTopics) {
          adminClient.deleteTopics(topicsToDelete).all().get();
        } else {
          System.out.println("Skipping deletion of topics as -d parameter wasn't specified!");
        }
        adminClient.createPartitions(operations).all().get();
        adminClient.alterConfigs(configChangeOperations).all().get();
        replicationManager.executeOperations();
      } catch (InterruptedException | ExecutionException e) {
        failedOperations++;
        System.err
            .println("Exceptions ocurred during execution of an operation: " + e.getMessage());
      }
    } else {
      System.out.println("Running in simulation mode, bypassing execute step!");
    }
    if (failedOperations > 0) {
      System.err.println("There were " + failedOperations + " failed operations when executing the necessary changes on the target cluster.");

      System.exit(1);
    }
  }

  private boolean compareTopics(NewTopic a, NewTopic b) {
    return a.name().equals(b.name()) && a.numPartitions() == b.numPartitions()
        && a.replicationFactor() == b.replicationFactor() && compareTopicConfig(a.configs(),
        b.configs());
  }

  private boolean compareTopicConfig(Map<String, String> sourceConfig,
      Map<String, String> targetConfig) {
    if (sourceConfig.keySet().equals(targetConfig.keySet())) {
      List<String> nonMatchingSettings = targetConfig.keySet().stream()
          .filter(setting -> !targetConfig.get(setting).equals(sourceConfig.get(setting)))
          .collect(Collectors.toList());
      return nonMatchingSettings.isEmpty();
    }
    return false;
  }


  @Override
  public void close() {
    adminClient.close();
  }
}
