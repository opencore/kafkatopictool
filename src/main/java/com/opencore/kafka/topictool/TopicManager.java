package com.opencore.kafka.topictool;

import com.opencore.kafka.topictool.output.OutputFormatService;
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
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.resource.ResourcePatternFilter;

public class TopicManager implements AutoCloseable {
  private Properties adminClientProperties;
  private AdminClient adminClient;
  private ReplicationFactorManager replicationManager;
  private String ignoreTopics;

  private Map<String, OutputFormatService> availableFormatters;

  public TopicManager(Properties adminClientProperties) {
    this.adminClientProperties = adminClientProperties;
    adminClient = AdminClient.create(adminClientProperties);

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
      Map<String, TopicDescription> all = adminClient.describeTopics(topicNames).all().get();

      // Retrieve config for all topics
      List<ConfigResource> configResourceList = all.keySet()
          .stream()
          .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
          .collect(Collectors.toList());
      Map<ConfigResource, Config> map = adminClient.describeConfigs(configResourceList).all().get();

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
        Collection<Node> nodeList = adminClient.describeCluster().nodes().get();
        List<String> list = nodeList.stream().map(e -> e.idString()).collect(Collectors.toList());
        ConfigResource firstBroker = new ConfigResource(ConfigResource.Type.BROKER, list.get(0));
        Map<ConfigResource, Config> configResourceConfigMap1 = adminClient.describeConfigs(Collections.singletonList(firstBroker)).all().get();
        configEntries = configResourceConfigMap1.get(firstBroker).entries().stream().filter(e -> e.value() != null).collect(Collectors.toMap(e -> e.name(), e -> e.value()));

        System.out.println();
      } catch (Exception e) {
        e.printStackTrace();
      }
      final Map<String, String> finalConfigEntries = configEntries;
      // Determine non-default configuration for every topic and add these to the NewTopic object
      for (NewTopic topic : newTopicList) {
        // Retrieve configuration for this topic and filter out any that are set to the default value
        Config topicConfig = configAsStringMap.get(topic.name());
        List<ConfigEntry> collect = topicConfig.entries().stream().filter(e -> finalConfigEntries.containsKey("log." + e.name())).collect(Collectors.toList());
        Map<String, String> collect1 = collect.stream().collect(Collectors.toMap(e -> e.name(), e -> e.value()));

        Map<String, String> filteredConfigs = topicConfig.entries().stream()
            .filter(entry -> !(entry.isDefault() && false))
            .collect(Collectors.toMap(e -> e.name(), e -> e.value()));
        topic.configs(filteredConfigs);
        System.out.println();
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return newTopicList;
  }

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

  public Set<String> getAvailableOutputFormats() {
    return availableFormatters.keySet();
  }

  public void sync(List<NewTopic> targetTopicList) {
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

    // Filter existing topics out of target state, and remove ignored topics, result of this operation are topics that need to be deleted
    Set<String> topicsToDelete = currentTopicList
        .stream()
        .filter(e -> !targetTopicMap.containsKey(e.name()))
        .filter(e -> !e.name().matches(ignoreTopics))
        .map(e -> e.name())
        .collect(Collectors.toSet());
    // TODO: identify and remove topics already marked for deletion


    // Filter topics that exist in current and target state and are not equal (by our definition) -> need to be modified
    Set<String> topicsToModify = currentTopicMap.keySet()
        .stream()
        .filter(e -> targetTopicMap.containsKey(e)) // topic names that exist in both the target and current state
        .filter(e -> !compareTopics(currentTopicMap.get(e), targetTopicMap.get(e)))
        .collect(Collectors.toSet());

    System.out.println("Creating: " + topicsToCreate.toString());
    adminClient.createTopics(topicsToCreate);

    System.out.println("Deleting: " + topicsToDelete.toString());
    adminClient.deleteTopics(topicsToDelete);

    System.out.println("Modifying: " + topicsToModify.toString());

    // Identify changes that are necessary changes for all topics to be modified
    Map<String, NewPartitions> operations = new HashMap<>();
    for (String topicToModify : topicsToModify) {
      NewTopic currentState = currentTopicMap.get(topicToModify);
      NewTopic targetState = targetTopicMap.get(topicToModify);

      // Check if partition count is different
      if (currentState.numPartitions() > targetState.numPartitions()) {
        System.out.println("Error: cannot reduce number of partitions for topic " + topicToModify + " from " + currentState.numPartitions() + " to " + targetState.numPartitions() + " - skipping this change!");
      } else if (currentState.numPartitions() < targetState.numPartitions()) {
        System.out.println("Queuing operation to add " + (targetState.numPartitions() - currentState.numPartitions()) + " partitions for topic " + topicToModify);
        operations.put(topicToModify, NewPartitions.increaseTo(targetState.numPartitions()));
      }

      // Check if replication factor is different
      if (currentTopicMap.get(topicToModify).replicationFactor() != targetTopicMap.get(topicToModify).replicationFactor()) {
        System.out.println("Changing replication factor for topic " + topicToModify);
        try {
          Map<String, TopicDescription> map = adminClient.describeTopics(Collections.singletonList(topicToModify)).all().get();
          replicationManager.addOperation(map.get(topicToModify), targetTopicMap.get(topicToModify).replicationFactor());
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
      }
    }

    // Execute operations
    try {
      adminClient.createPartitions(operations).all().get();
      replicationManager.executeOperations();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }


    // ==== Manage ACLs
    AclBindingFilter allTopics = new AclBindingFilter(ResourcePatternFilter.ANY, AccessControlEntryFilter.ANY);

    try {
      Collection<AclBinding> aclList = adminClient.describeAcls(allTopics).values().get();
      System.out.println();
    } catch (SecurityDisabledException se) {
      System.out.println("No authorizer is configured on the broker, skipping ACL sync step.");
    } catch (InterruptedException ie) {
      System.out.println("InterruptedException: " + ie.getMessage());
    } catch (ExecutionException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof SecurityDisabledException) {
        System.out.println("No authorizer is configured on the broker, skipping ACL sync step.");
      } else {
        System.out.println("ExecutionException: " + ex.getMessage());
      }
    }

  }

  private boolean compareTopics(NewTopic a, NewTopic b) {
    // TODO: hack and incomplete
    return a.name().equals(b.name()) && a.numPartitions() == b.numPartitions() && a.replicationFactor() == b.replicationFactor();
  }


  @Override
  public void close() {
    adminClient.close();
  }
}
