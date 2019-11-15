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

import com.opencore.kafka.topictool.command.Action;
import com.opencore.kafka.topictool.command.CompareCommand;
import com.opencore.kafka.topictool.command.ExportCommand;
import com.opencore.kafka.topictool.command.SyncCommand;
import com.opencore.kafka.topictool.command.TopicToolCommand;
import com.opencore.kafka.topictool.command.TopicToolCommandResult;
import com.opencore.kafka.topictool.output.OutputFormatService;
import com.opencore.kafka.topictool.repository.TopicDefinition;
import com.opencore.kafka.topictool.repository.provider.RepositoryProviderService;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class that provides the functionality to administer topics and ACLs in Kafka.
 *
 * <p>This will be instantiated by clients and passed TopicToolCommand objects that describe the
 * actions to be taken.
 */
public class TopicTool {

  private Logger logger = LoggerFactory.getLogger(TopicTool.class);

  private TopicToolConfig config = null;
  private HashMap<String, TopicManager> topicManagerMap = null;
  private HashMap<String, RepositoryProviderService> repositoryMap = null;

  public static Properties cleanupAdminClientProperties(Properties adminClientProperties) {
    // Create a new copy of the Properties object
    // which only contains the allowed config
    // values for an AdminClient
    Properties result = new Properties();
    List<String> validSettings = adminClientProperties.keySet()
        .stream()
        .filter(e -> e instanceof String)
        .map(e -> (String) e)
        .filter(e -> AdminClientConfig.configNames().contains(e))
        .collect(Collectors.toList());

    for (String setting : validSettings) {
      result.setProperty(setting, adminClientProperties.getProperty(setting));
    }
    return result;
  }

  public static AdminClient getAdminClient(Properties adminClientProperties) {
    return AdminClient.create(cleanupAdminClientProperties(adminClientProperties));
  }

  public TopicTool(TopicToolConfig config) {
    this.config = config;
    this.topicManagerMap = new HashMap<>();
    this.repositoryMap = new HashMap<>();

    for (String clusterName : config.getClusterNames()) {
      topicManagerMap
          .put(clusterName, new TopicManager(clusterName, config.getClusterConfig(clusterName)));
    }

    for (String repositoryName : config.getRepoNames()) {
      Properties repositoryConfig = config.getRepoConfig(repositoryName);
      String repositoryType = repositoryConfig.getProperty("provider");
      if (repositoryType == null) {
        // No provider type was specified in the config, nothing we can do with this
        // repo config, we'll ignore it
        logger.warn(
            "Ignoring repository " + repositoryName + " due to missing provider configuration.");
        continue;
      }
      repositoryMap.put(repositoryName, loadRepoProvider(repositoryType, repositoryConfig));
    }
  }

  public static RepositoryProviderService loadRepoProvider(String repositoryType, Properties config) {
    ServiceLoader<RepositoryProviderService> loader =
        ServiceLoader.load(RepositoryProviderService.class);
    Iterator<RepositoryProviderService> serviceIterator = loader.iterator();

    while (serviceIterator.hasNext()) {
      RepositoryProviderService currentProvider = serviceIterator.next();
      if (currentProvider.repositoryProviderType().matches(repositoryType)) {
        currentProvider.configure(config);
        return currentProvider;
      }
    }
    System.out.println("Unable to find service provider for repository of type " + repositoryType);
    return null;
  }

  public static OutputFormatService loadOutputFormatter(String outputFormat) {
    HashMap<String, OutputFormatService> availableFormatters = getOutputFormatters();
    if (!availableFormatters.containsKey(outputFormat)) {
      System.out.println("Unable to find service provider for output format: " + outputFormat);
    }
    return availableFormatters.get(outputFormat);
  }

  public static List<String> getOutputFormatList() {
    List<String> result = new ArrayList<>();
    result.addAll(getOutputFormatters().keySet());
    return result;
  }

  private static HashMap<String, OutputFormatService> getOutputFormatters() {
    HashMap<String, OutputFormatService> availableFormatters = new HashMap<>();

    ServiceLoader<OutputFormatService> loader = ServiceLoader.load(OutputFormatService.class);
    Iterator<OutputFormatService> serviceIterator = loader.iterator();

    while (serviceIterator.hasNext()) {
      OutputFormatService currentFormatter = serviceIterator.next();
      availableFormatters.put(currentFormatter.formatName(), currentFormatter);
    }
    return availableFormatters;
  }

  private TopicToolCommandResult executeCompare(CompareCommand command) {
    boolean printMismatchOnly =
        config.getConfig().getBoolean(TopicToolConfig.MISMATCHONLY_OPTION_NAME);
    boolean detailed = config.getConfig().getBoolean(TopicToolConfig.PRINTDETAILED_OPTION_NAME);

    List<String> topics = config.getList(TopicToolConfig.TOPICPATTERN_OPTION_NAME);
    TopicComparer comparer = new TopicComparer(config.getClusterConfigs(),
        config.getConfig().getInt(TopicToolConfig.THREADCOUNT_OPTION_NAME));

    List<String> clusterList = new ArrayList<>();
    clusterList.addAll(config.getClusterConfigs().keySet());
    TopicCompareResult result = comparer.compare(topics, clusterList);

    Map<String, List<PartitionCompareResult>> resultMap = result.all();
    System.out.println("Compared " + resultMap.size() + " topics..");
    for (String topic : resultMap.keySet().stream().sorted().collect(Collectors.toList())) {
      boolean match = true;
      for (PartitionCompareResult partitionResult : resultMap.get(topic)) {
        match = match && partitionResult.isMatch();
        if (detailed) {
          System.out.println(
              "Partition " + partitionResult.getPartition() + ": " + partitionResult.toString());
        }
      }
      if (!printMismatchOnly || !match) {
        System.out.println(topic + ": " + (match ? "MATCH" : "MISMATCH"));
      }

    }
    comparer.close();

    return new TopicToolCommandResult();
  }

  private TopicToolCommandResult executeSync(SyncCommand command) {
    // Initialize repository provider
    String repositoryName = command.getSourceRepository();
    Properties repositoryConfig = config.getRepoConfig(repositoryName);
    String repositoryType = repositoryConfig.getProperty("provider");

    RepositoryProviderService repositoryProvider =
        loadRepoProvider(repositoryType, config.getRepoConfig(repositoryName));

    // Initialize cluster topic manager
    String clusterName = command.getTargetCluster();
    TopicManager topicManager = topicManagerMap.get(clusterName);

    // Retrieve the defined topics from the repository
    Map<String, TopicDefinition> topicList = repositoryProvider.getTopics(command.getTopicPattern());


    // Convert to list in the format that TopicManager expects
    List<NewTopic> tl = topicList.values()
        .stream()
        .map(e -> e.getNewTopic())
        .collect(Collectors.toList());

    // Execute sync command
    topicManager.sync(tl, command.getSimulate());

    return new TopicToolCommandResult();
  }

  private TopicToolCommandResult executeExport(ExportCommand command) {
    String clusterName = command.getSourceCluster();
    TopicManager clusterManager = topicManagerMap.get(clusterName);
    List<TopicDefinition> topicsToExport = clusterManager.getTopics()
        .stream()
        .map(e -> new TopicDefinition(e, null))
        .collect(Collectors.toList());

    OutputFormatService outputFormatter = TopicTool.loadOutputFormatter(command.getOutputFormat());
    if (outputFormatter == null) {
      logger.error("Unable to load outputformatter for format " + command.getOutputFormat() + "skipping export for cluster " + clusterName);
      return new TopicToolCommandResult();
    }
    try (Writer writer = new OutputStreamWriter(new FileOutputStream(command.getFilePrefix() + clusterName + ".json"),StandardCharsets.UTF_8)) {
      outputFormatter.format(topicsToExport, writer);
    } catch (IOException e) {
      System.out.println(e.getMessage());
    }
    return new TopicToolCommandResult();
  }


  public List<TopicToolCommandResult> execute(List<TopicToolCommand> commands) {
    List<TopicToolCommandResult> result = new ArrayList<>();

    for (TopicToolCommand currentCommand : commands) {
      result.add(execute(currentCommand));
    }
    return result;
  }

  public TopicToolCommandResult execute(TopicToolCommand command) {
    switch (command.getAction()) {
      case Action.COMPARE:
        return (executeCompare((CompareCommand) command));

      case Action.EXPORT:
        return (executeExport((ExportCommand) command));

      case Action.SYNC:
        return (executeSync((SyncCommand) command));

      default:
        // Unknown action type, either an error or maybe
        // a version mismatch
        System.out.println("Unknown command type encountered, skipping action!");
    }

    return new TopicToolCommandResult();
  }

  public boolean isClusterConfigured(String clusterName) {
    return topicManagerMap.containsKey(clusterName);
  }

  public boolean isRepoConfigured(String repoName) {
    return repositoryMap.containsKey(repoName);
  }

}
