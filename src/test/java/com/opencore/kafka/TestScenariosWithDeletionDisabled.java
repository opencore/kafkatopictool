package com.opencore.kafka;


import com.opencore.kafka.testutils.AbstractTestCase;
import com.opencore.kafka.topictool.repository.TopicDefinition;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Test;

public class TestScenariosWithDeletionDisabled extends AbstractTestCase {

  public static final boolean SIMULATE = false;
  private boolean deleteTopics = false;

  @Test
  public void testScenario1WithDeletionEnabled() throws Exception {
    runScenario("1", SIMULATE, deleteTopics, "expectedtargetstatenodeletion.json");
  }

  @Test
  public void testScenario2WithDeletionEnabled() throws Exception {
    runScenario("2", SIMULATE, deleteTopics);
  }

  @Test
  public void testScenario3WithDeletionEnabled() throws Exception {
    runScenario("3", SIMULATE, deleteTopics);
  }

  @Test
  public void testScenario4WithDeletionEnabled() throws Exception {
    runScenario("4", SIMULATE, deleteTopics);
  }

  @Test
  public void testScenario5WithDeletionEnabled() throws Exception {
    runScenario("5", SIMULATE, deleteTopics, "expectedtargetstatenodeletion.json");
  }

  @Test
  public void testScenario6WithDeletionEnabled() throws Exception {
    runScenario("6", SIMULATE, deleteTopics, "expectedtargetstate.json");
  }

  @Test
  public void testScenario7WithDeletionEnabled() throws Exception {
    runScenario("7", SIMULATE, deleteTopics);
  }

  @Test
  public void testScenario8WithDeletionEnabled() throws Exception {
    runScenario("8", SIMULATE, deleteTopics, "expectedtargetstate.json");
  }

}
