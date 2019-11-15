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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.impl.action.AppendArgumentAction;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class TopicToolConfig {

  public static final String CLUSTER_PREFIX = "cluster";
  public static final String REPO_PREFIX = "repository";

  // Define values for positional command argument
  public static final String COMMAND_OPTION_NAME = "command";
  public static final String COMPARE_COMMAND_NAME = "compare";
  public static final String SYNC_COMMAND_NAME = "sync";
  public static final String EXPORT_COMMAND_NAME = "export";

  //
  public static final String CLUSTER_OPTION_NAME = "cluster";
  public static final String CONFIGFILE_OPTION_NAME = "configfile";
  public static final String SIMULATE_OPTION_NAME = "simulate";
  public static final String MISMATCHONLY_OPTION_NAME = "mismatchonly";
  public static final String TOPICPATTERN_OPTION_NAME = "topics";
  public static final String REPOSITORY_OPTION_NAME = "repository";
  public static final String PRINTDETAILED_OPTION_NAME = "detailed";
  public static final String EXPORTFILEPREFIX_OPTION_NAME = "exportfileprefix";

  public static final String OUTPUTFORMAT_OPTION_NAME = "outputformat";
  public static final String OUTPUTFORMAT_OPTION_DEFAULT = "json";

  public static final String THREADCOUNT_OPTION_NAME = "threadcount";
  public static final int THREADCOUNT_OPTION_DEFAULT = 5;

  private Properties rawProps;
  private Map<String, Properties> clusterProperties;
  private Map<String, Properties> repoProperties;
  private Namespace parsedCommandLineArgs;

  public TopicToolConfig(String[] commandLineArgs) {
    initialize(commandLineArgs);
  }

  public Namespace getConfig() {
    return parsedCommandLineArgs;
  }

  public List<String> getList(String listName) {
    return parsedCommandLineArgs.getList(listName);
  }

  public void initialize(String[] commandLineArgs) {
    Properties adminClientProps = new Properties();
    final ArgumentParser argumentParser = ArgumentParsers.newFor("TopicTool").build();

    Subparsers subparsers = argumentParser.addSubparsers()
        .help("list topics in a variety of formats.");

    Subparser compareParser =
        subparsers.addParser(COMPARE_COMMAND_NAME).setDefault(COMMAND_OPTION_NAME,
            COMPARE_COMMAND_NAME);
    compareParser.addArgument("-f", "--config-file")
        .dest(CONFIGFILE_OPTION_NAME)
        .type(Arguments.fileType().acceptSystemIn().verifyCanRead())
        .help("Path and name of file to load Kafka configuration from.");
    compareParser.addArgument("-c", "--cluster")
        .dest(CLUSTER_OPTION_NAME)
        .action(new AppendArgumentAction())
        .help("Cluster from config file that should be queried.");
    compareParser.addArgument("-p", "--topic-pattern")
        .dest(TOPICPATTERN_OPTION_NAME)
        .action(new AppendArgumentAction())
        .help("Topics to compare, takes regexes: for example test.* or test.*|xxx.*");
    compareParser.addArgument("-t", "--threads")
        .dest(THREADCOUNT_OPTION_NAME)
        .type(Integer.class)
        .setDefault(THREADCOUNT_OPTION_DEFAULT)
        .help("Number of threads to start for comparison operations.");
    compareParser.addArgument("-m", "--mismatch-only")
        .dest(MISMATCHONLY_OPTION_NAME)
        .action(Arguments.storeTrue())
        .help("Print only topics that don't match.");
    compareParser.addArgument("-d", "--detailed")
        .dest(PRINTDETAILED_OPTION_NAME)
        .action(Arguments.storeTrue())
        .help("Print information per partition.");

    Subparser exportParser =
        subparsers.addParser(EXPORT_COMMAND_NAME).setDefault(COMMAND_OPTION_NAME,
            EXPORT_COMMAND_NAME);
    exportParser.addArgument("-f", "--config-file")
        .dest(CONFIGFILE_OPTION_NAME)
        .type(Arguments.fileType().acceptSystemIn().verifyCanRead())
        .help("Path and name of file to load Kafka configuration from.");
    exportParser.addArgument("-c", "--cluster")
        .dest(CLUSTER_OPTION_NAME)
        .action(new AppendArgumentAction())
        .help("Cluster from config file that should be queried.");
    exportParser.addArgument("-p", "--prefix")
        .dest(EXPORTFILEPREFIX_OPTION_NAME)
        .setDefault("")
        .help("Prefix to add to export file - file name will be \"<prefix><clustername>.json\"");
    exportParser.addArgument("-o", "--output-format")
        .dest(OUTPUTFORMAT_OPTION_NAME)
        .setDefault(OUTPUTFORMAT_OPTION_DEFAULT)
        .help("Output format to use for formatting the export file. Available formatters are: "
            + TopicTool.getOutputFormatList().toString());

    Subparser syncParser = subparsers.addParser(SYNC_COMMAND_NAME).setDefault(COMMAND_OPTION_NAME,
        SYNC_COMMAND_NAME);
    syncParser.addArgument("-f", "--config-file")
        .dest(CONFIGFILE_OPTION_NAME)
        .required(true)
        .type(Arguments.fileType().acceptSystemIn().verifyCanRead())
        .help("Path and name of file to load Kafka and repository configuration from.");
    syncParser.addArgument("-c", "--cluster")
        .dest(CLUSTER_OPTION_NAME)
        .action(new AppendArgumentAction())
        .help(
            "Limit sync to the specified clusters, if not specified all clusters defined in the config file will be targeted.");
    syncParser.addArgument("-r", "--repository")
        .dest(REPOSITORY_OPTION_NAME)
        .required(true)
        .help(
            "The name of the repository that holds the target state definition to be created on the clusters.");
    syncParser.addArgument("-s", "--simulate")
        .dest(SIMULATE_OPTION_NAME)
        .action(Arguments.storeTrue())
        .help("Don't execute sync actions, print differences only.");
    syncParser.addArgument("-p", "--topic-pattern")
        .dest(TOPICPATTERN_OPTION_NAME)
        .action(new AppendArgumentAction())
        .help("Limit topics to sync, takes regexes: for example test.* or test.*|xxx.*");

    try {
      parsedCommandLineArgs = argumentParser.parseArgs(commandLineArgs);
    } catch (ArgumentParserException e) {
      System.out.println("Error when parsing command line arguments: " + e.getMessage());
      System.exit(1);
    }

    // Read config file if one was specified
    File configFile = parsedCommandLineArgs.get("configfile");
    if (configFile != null) {
      try (Reader configReader = new InputStreamReader(new FileInputStream(configFile),
          StandardCharsets.UTF_8)) {
        adminClientProps.load(configReader);
      } catch (FileNotFoundException e) {
        System.out
            .println("Config file not found: " + parsedCommandLineArgs.getString("configfile"));
      } catch (IOException e) {
        System.out.println(
            "Something went wrong when parsing the provided configuration file: " + e.getMessage());
      }
    }

    // Override bootstrap server property if present
    String bootstrapServer = parsedCommandLineArgs.getString("bootstrapServer");
    if (bootstrapServer != null) {
      adminClientProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    }

    // Add extra parameters
    List<String> extraParameters = parsedCommandLineArgs.getList("parameters");
    if (extraParameters != null) {
      for (String param : extraParameters) {
        String[] keyValue = param.split("=");
        if (keyValue.length != 2) {
          System.out
              .println("Couldn't parse value \"" + param + "\" as key=value pair, ignoring ... ");
          continue;
        }
        adminClientProps.setProperty(keyValue[0], keyValue[1]);
      }
    }

    // Set objects
    this.rawProps = adminClientProps;
    this.clusterProperties = getMapWithNameByPrefix(CLUSTER_PREFIX, rawProps);
    this.repoProperties = getMapWithNameByPrefix(REPO_PREFIX, rawProps);
    return;
  }

  public Set<String> getClusterNames() {
    return this.clusterProperties.keySet();
  }

  public Map<String, Properties> getClusterConfigs() {
    return clusterProperties;
  }

  public Properties getClusterConfig(String clusterName) {
    return clusterProperties.get(clusterName);
  }

  public Set<String> getRepoNames() {
    return repoProperties.keySet();
  }

  public Map<String, Properties> getRepoConfigs() {
    return repoProperties;
  }

  public Properties getRepoConfig(String repoName) {
    return repoProperties.get(repoName);
  }

  private Map<String, Properties> getMapWithNameByPrefix(String prefix,
      Properties inputProperties) {
    // Add trailing . to prefix if not present
    final String prefixString = prefix.endsWith(".") ? prefix : prefix.concat(".");

    // filter out all properties that start with the prefix, remove the prefix and create a new properties object
    Map<String, String> allClusterProperties = rawProps.keySet()
        .stream()
        .map(e -> e.toString())
        .filter(e -> e.startsWith(prefixString))
        .map(e -> e.substring(prefixString.length()))// remove leading "cluster."
        .filter(e -> e.indexOf(".")
            != -1) // remove any properties that don't contain a . which delimits the cluster name
        .collect(Collectors.toMap(e -> e, e -> rawProps.getProperty(prefixString + e)));

    // get list of all names
    List<String> definedClusters = allClusterProperties.keySet()
        .stream()
        .map(e -> e.substring(0, e.indexOf(".")))
        .distinct()
        .collect(Collectors.toList());

    Map<String, Properties> result = new HashMap<>();
    for (String namePrefix : definedClusters) {
      Map<String, String> map = allClusterProperties.keySet()
          .stream()
          .filter(e -> e.startsWith(namePrefix))
          .collect(Collectors
              .toMap(e -> e.substring(namePrefix.length() + 1), e -> allClusterProperties.get(e)));

      // Convert to Properties object and store
      Properties scopedProperties = new Properties();
      scopedProperties.putAll(map);
      result.put(namePrefix, scopedProperties);

    }
    return result;
  }

  public Properties getFullConfig() {
    return rawProps;
  }

}
