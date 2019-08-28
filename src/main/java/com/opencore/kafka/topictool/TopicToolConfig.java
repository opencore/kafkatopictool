package com.opencore.kafka.topictool;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

  private Properties rawProps;
  private Map<String, Properties> clusterProperties;
  private Map<String, Properties> repoProperties;
  private Namespace parsedCommandLineArgs;

  public TopicToolConfig(String[] commandLineArgs) {
    initialize(commandLineArgs);
  }

  public String getConfig(String configName) {
    return parsedCommandLineArgs.get(configName);
  }

  public List<String> getList(String listName) {
    return parsedCommandLineArgs.getList(listName);
  }

  public void initialize(String[] commandLineArgs) {
    Properties adminClientProps = new Properties();
    final ArgumentParser argumentParser = ArgumentParsers.newFor("TopicTool").build();

    Subparsers subparsers = argumentParser.addSubparsers().help("list topics in a variety of formats.");

    Subparser exportParser = subparsers.addParser("export").setDefault("command", "export");
    exportParser.addArgument("-f", "--config-file")
        .dest("configfile")
        .type(Arguments.fileType().acceptSystemIn().verifyCanRead())
        .help("Path and name of file to load Kafka configuration from.");
    exportParser.addArgument("-c", "--cluster")
        .dest("cluster")
        .action(new AppendArgumentAction())
        .help("Cluster from config file that should be queried.");
    exportParser.addArgument("-b", "--bootstrap-servers")
        .dest("bootstrapServer")
        .help("Allows using the tool without creating a config file, convenience parameter (equivalent to -p \"bootstrap.servers=...\") to specify the bootstrap servers.");
    exportParser.addArgument("-p", "--parameter")
        .dest("parameters")
        .action(new AppendArgumentAction())
        .help("Extra parameters when not using a config file.");

    Subparser syncParser = subparsers.addParser("sync").setDefault("command", "sync");;
    syncParser.addArgument("-f", "--config-file")
        .dest("configfile")
        .required(true)
        .type(Arguments.fileType().acceptSystemIn().verifyCanRead())
        .help("Path and name of file to load Kafka and repository configuration from.");
    syncParser.addArgument("-c", "--cluster")
        .dest("cluster")
        .action(new AppendArgumentAction())
        .help("Limit sync to the specified clusters, if not specified all clusters defined in the config file will be targeted.");

    /*Subparser serverParser = subparsers.addParser("server").setDefault("command", "server");;
    serverParser.addArgument("-f", "--config-file")
        .dest("configfile")
        .required(true)
        .type(Arguments.fileType().acceptSystemIn().verifyCanRead())
        .help("Path and name of file to load Kafka and repository configuration from.");
    */
    try {
      parsedCommandLineArgs = argumentParser.parseArgs(commandLineArgs);
    } catch (ArgumentParserException e) {
      System.out.println("Error when parsing command line arguments: " + e.getMessage());
      System.exit(1);
    }

    // Read config file if one was specified
    File configFile = parsedCommandLineArgs.get("configfile");
    if (configFile != null) {
      try {
        FileReader configReader = new FileReader(configFile);
        adminClientProps.load(configReader);
      } catch (FileNotFoundException e) {
        System.out.println("Config file not found: " + parsedCommandLineArgs.getString("configfile"));
      } catch (IOException e) {
        System.out.println("Something went wrong when parsing the provided configuration file: " + e.getMessage());
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
          System.out.println("Couldn't parse value \"" + param + "\" as key=value pair, ignoring ... ");
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

  public Map<String, Properties> getClusterConfigs() {
    return clusterProperties;
  }

  public Properties getClusterConfig(String clusterName) {
    return clusterProperties.get(clusterName);
  }

  public Map<String, Properties> getRepoProperties() {
    return repoProperties;
  }

  private Map<String, Properties> getMapWithNameByPrefix(String prefix, Properties inputProperties) {
    // Add trailing . to prefix if not present
    final String prefixString = prefix.endsWith(".") ? prefix : prefix.concat(".");

    // filter out all properties that start with the prefix, remove the prefix and create a new properties object
    Map<String, String> allClusterProperties = rawProps.keySet()
        .stream()
        .map(e -> e.toString())
        .filter(e -> e.startsWith(prefixString))
        .map(e -> e.substring(prefixString.length()))// remove leading "cluster."
        .filter(e -> e.indexOf(".") != -1) // remove any properties that don't contain a . which delimits the cluster name
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
          .collect(Collectors.toMap(e -> e.substring(namePrefix.length() + 1), e -> allClusterProperties.get(e)));

      // Convert to Properties object and store
      Properties scopedProperties = new Properties();
      scopedProperties.putAll(map);
      result.put(namePrefix, scopedProperties);

    }
    System.out.println(allClusterProperties);
    return result;
  }

  public Properties getFullConfig() {
    return rawProps;
  }

}
