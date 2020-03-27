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

public class TestScenariosWithSimulationEnabled extends AbstractTestCase {

  private boolean deleteTopics = true;
  private boolean simulate = true;

  @Test
  public void testScenario1WithSimulationEnabled() throws Exception {
    runScenario("1", simulate, deleteTopics, "initialtargetstate.json");
  }

  @Test
  public void testScenario2WithSimulationEnabled() throws Exception {
    runScenario("2", simulate, deleteTopics, "initialtargetstate.json");
  }

  @Test
  public void testScenario3WithSimulationEnabled() throws Exception {
    runScenario("3", simulate, deleteTopics, "initialtargetstate.json");
  }

  @Test
  public void testScenario4WithSimulationEnabled() throws Exception {
    runScenario("4", simulate, deleteTopics, "initialtargetstate.json");
  }

  @Test
  public void testScenario5WithSimulationEnabled() throws Exception {
    runScenario("5", simulate, deleteTopics, "initialtargetstate.json");
  }

  @Test
  public void testScenario6WithSimulationEnabled() throws Exception {
    runScenario("6", simulate, deleteTopics, "initialtargetstate.json");
  }

  @Test
  public void testScenario7WithSimulationEnabled() throws Exception {
    runScenario("7", simulate, deleteTopics, "initialtargetstate.json");
  }

  @Test
  public void testScenario8WithSimulationEnabled() throws Exception {
    runScenario("8", simulate, deleteTopics, "initialtargetstate.json");
  }
}
