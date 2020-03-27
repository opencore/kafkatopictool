package com.opencore.kafka;


import com.opencore.kafka.testutils.AbstractTestCase;
import org.junit.Test;

public class TestScenariosWithDeletionEnabled extends AbstractTestCase {

  private boolean deleteTopics = true;

  @Test
  public void testScenario1WithDeletionEnabled() throws Exception {
    runScenario("1", false, deleteTopics);
  }

  @Test
  public void testScenario2WithDeletionEnabled() throws Exception {
    runScenario("2", false, deleteTopics);
  }

  @Test
  public void testScenario3WithDeletionEnabled() throws Exception {
    runScenario("3", false, deleteTopics);
  }

  @Test
  public void testScenario4WithDeletionEnabled() throws Exception {
    runScenario("4", false, deleteTopics);
  }

  @Test
  public void testScenario5WithDeletionEnabled() throws Exception {
    runScenario("5", false, deleteTopics);
  }

  @Test
  public void testScenario6WithDeletionEnabled() throws Exception {
    runScenario("6", false, deleteTopics, "expectedtargetstate.json");
  }

  @Test
  public void testScenario7WithDeletionEnabled() throws Exception {
    runScenario("7", false, deleteTopics);
  }

  @Test
  public void testScenario8WithDeletionEnabled() throws Exception {
    runScenario("8", false, deleteTopics, "expectedtargetstate.json");
  }
}
