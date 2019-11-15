package com.opencore.kafka.topictool;

import com.google.common.collect.Sets;
import com.opencore.kafka.topictool.command.CompareCommand;
import com.opencore.kafka.topictool.command.ExportCommand;
import com.opencore.kafka.topictool.command.SyncCommand;
import com.opencore.kafka.topictool.command.TopicToolCommand;
import com.opencore.kafka.topictool.command.TopicToolCommandResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CommandLineClient {

  public static void main(String[] args) {
    TopicToolConfig config = new TopicToolConfig(args);

    TopicTool tool = new TopicTool(config);

    List<TopicToolCommand> commands = new ArrayList<>();
    Boolean simulate = config.getConfig().getBoolean(TopicToolConfig.SIMULATE_OPTION_NAME);
    if (simulate == null) {
      simulate = false;
    }
    if (config.getConfig().getString(TopicToolConfig.COMMAND_OPTION_NAME)
        == TopicToolConfig.EXPORT_COMMAND_NAME) {
      // Get the list of clusters specified on the command line and create an
      // export command per cluster

      List<String> inScopeClusters =
          getAndCheckClusterList(config); // This exits if no clusters are passed
      for (String currentCluster : inScopeClusters) {
        if (!tool.isClusterConfigured(currentCluster)) {
          System.out.println("The cluster " + currentCluster
              + " is not specified in the current config file, no action will be taken for this cluster.");
          continue;
        }
        // At this point we are sure that we know this cluster and can schedule the export
        String filePrefix =
            config.getConfig().getString(TopicToolConfig.EXPORTFILEPREFIX_OPTION_NAME);
        String outputFormat =
            config.getConfig().getString(TopicToolConfig.OUTPUTFORMAT_OPTION_NAME);
        commands.add(
            new ExportCommand(currentCluster, currentCluster + ".json", simulate, filePrefix,
                outputFormat));
      }
    } else if (config.getConfig().getString(TopicToolConfig.COMMAND_OPTION_NAME)
        == TopicToolConfig.SYNC_COMMAND_NAME) {
      List<String> inScopeClusters =
          getAndCheckClusterList(config); // This exits if no clusters are passed

      String sourceRepository =
          config.getConfig().getString(TopicToolConfig.REPOSITORY_OPTION_NAME);

      if (!tool.isRepoConfigured(sourceRepository)) {
        System.out.println(
            "Specified repository " + sourceRepository + " not found in config file, aborting!");
        System.exit(3);
      }
      for (String currentCluster : inScopeClusters) {
        if (!tool.isClusterConfigured(currentCluster)) {
          System.out.println("The cluster " + currentCluster
              + " is not specified in the current config file, no action will be taken for this cluster.");
          continue;
        }
        // At this point we are sure that we know this cluster and can schedule the sync
        commands.add(new SyncCommand(sourceRepository, currentCluster, simulate));
      }
    } else if (config.getConfig().getString(TopicToolConfig.COMMAND_OPTION_NAME)
        == TopicToolConfig.COMPARE_COMMAND_NAME) {
      List<String> inScopeClusters = getAndCheckClusterList(config);

      // We need nothing more, now we can generate a compare command for every
      // combination of clusters from the list specified on the command line
      // so for A, B, C we will compare
      // A -> B
      // A -> C
      // B -> C
      // In theory we should be fine to omit the last comparison, but that would
      // miss the case were only A is different.

      // Generate all possible powersets with size of two from the inscope clusters
      // I know, I know, it is incredibly lazy to pull in Guava as a dependency
      // just for this, but I just couldn't be bothered to implement something
      // myself
      List<Set<String>> comparisons = Sets.powerSet(new HashSet<String>(inScopeClusters))
          .stream()
          .filter(e -> e.size() == 2)
          .collect(Collectors.toList());

      for (Set<String> pairing : comparisons) {
        ArrayList<String> pairingList = new ArrayList<>(pairing);
        commands.add(new CompareCommand(pairingList.get(0), pairingList.get(1), simulate,
            config.getConfig().getBoolean(TopicToolConfig.MISMATCHONLY_OPTION_NAME)));
      }
    }

    // Commands were created, irrelevant what type of command they are
    // at this point we can simply execute them, check the results and
    // exit
    List<TopicToolCommandResult> commandResults = tool.execute(commands);

  }

  private static List<String> getAndCheckClusterList(TopicToolConfig config) {

    // Get the list of clusters specified on the command line and create an
    // export command per cluster
    List<String> inScopeClusters = config.getList(TopicToolConfig.CLUSTER_OPTION_NAME);
    if (inScopeClusters == null) {
      System.out.println("No cluster specified, aborting!");
      System.exit(2);
    }
    return inScopeClusters;
  }
}
