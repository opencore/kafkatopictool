/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.opencore.kafka.testutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.opencore.kafka.topictool.TopicManager;
import com.opencore.kafka.topictool.repository.TopicDefinition;
import com.opencore.kafka.topictool.repository.provider.KafkaRepositoryProvider;
import com.opencore.kafka.topictool.repository.provider.RepositoryProviderService;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.After;
import org.junit.Before;

/**
 * Test harness to run against a real, local Kafka cluster and REST proxy. This is essentially
 * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined and ported to Java with
 * the addition of the REST proxy. Defaults to a 1-ZK, 3-broker, 1 REST proxy cluster.
 */
public abstract class AbstractTestCase {

  public static final int TIMEOUT = 5;
  public static final String SCENARIO_DIR_PREFIX = "scenarios/";
  protected TestCluster sourceCluster;
  protected TestCluster targetCluster;

  protected AdminClient sourceAdminClient;
  protected AdminClient targetAdminClient;

  protected TopicManager targetTopicManager;
  protected RepositoryProviderService sourceRepo;


  public static final int DEFAULT_NUM_BROKERS = 1;

  public AbstractTestCase() {
    sourceCluster = new TestCluster();
    targetCluster = new TestCluster();
  }

  @Before
  public void setUp() throws Exception {
    sourceCluster.setUp();
    targetCluster.setUp(100);
    sourceAdminClient = getSourceAdminClient();
    targetAdminClient = getTargetAdminClient();

    targetTopicManager = getTargetTopicManager("target");
    sourceRepo = getSourceRepo();
  }

  @After
  public void tearDown() throws Exception {
    sourceAdminClient.close();
    targetAdminClient.close();
    targetTopicManager.close();

    sourceCluster.tearDown();
    targetCluster.tearDown();
  }

  protected String getSourceBootstrapServers() {
    return sourceCluster.getBootstrapServers();
  }

  protected String getSourceZKConnect() {
    return sourceCluster.getZKConnect();
  }

  protected String getTargetBootstrapServers() {
    return targetCluster.getBootstrapServers();
  }

  protected String getTargetZKConnect() {
    return targetCluster.getZKConnect();
  }



  protected void runScenario(String scenarioName) throws Exception {
    runScenario(scenarioName, false, false, null);
  }

  protected void runScenario(String scenarioName, String expectedTargetStateFile) throws Exception {
    runScenario(scenarioName, false, false, expectedTargetStateFile);
  }

  protected void runScenario(String scenarioName, boolean simulate, boolean deleteTopics) throws Exception {
    runScenario(scenarioName, simulate, deleteTopics, null);
  }

  protected void runScenario(String scenarioName, boolean simulate, boolean deleteTopics, String expectedTargetStateFile)
      throws Exception {
    String sourceStateFile = SCENARIO_DIR_PREFIX + scenarioName + "/sourcestate.json";
    String initialTargetStateFile = SCENARIO_DIR_PREFIX + scenarioName + "/initialtargetstate.json";

    if (expectedTargetStateFile == null) {
      expectedTargetStateFile = SCENARIO_DIR_PREFIX + scenarioName + "/sourcestate.json";
    } else {
      expectedTargetStateFile = SCENARIO_DIR_PREFIX + scenarioName + "/" + expectedTargetStateFile;
    }

    runTest(sourceStateFile, initialTargetStateFile, expectedTargetStateFile, simulate, deleteTopics);
  }

  protected void runTest(String sourceStateFile, String targetStateFile,
      String expectedTargetStateFile, boolean simulate, boolean deleteTopics) throws Exception {

    loadTargetState(targetStateFile);
    loadSourceState(sourceStateFile);

    // Give the clusters a little time to initialize partitions
    TimeUnit.SECONDS.sleep(TIMEOUT);

    System.out.println("target:" + targetAdminClient.listTopics().names().get().size());
    System.out.println("source:" + sourceAdminClient.listTopics().names().get().size());
    Map<String, TopicDefinition> topics = sourceRepo.getTopics(".*");
    targetTopicManager.sync(topics, simulate, deleteTopics);
    TimeUnit.SECONDS.sleep(TIMEOUT);
    Collection<NewTopic> expectedTopics = loadFromFile(expectedTargetStateFile);
    Collection<String> existingTopicNames = targetAdminClient.listTopics().names().get();

    // Check whether the number of topics is the same in expected target
    // state and target cluster
    assertEquals(expectedTopics.size(), existingTopicNames.size());

    // Retrieve more detailed information for topics on target cluster
    Map<String, TopicDescription> existingTopicDetails =
        targetAdminClient.describeTopics(existingTopicNames).all().get();

    List<NewTopic> tl = existingTopicDetails.values()
        .stream()
        .map(e -> new NewTopic(e.name(), e.partitions().size(),
            (short) e.partitions().get(0).replicas().size()))
        .collect(Collectors.toList());

    Map<String, NewTopic> existingTopicDetailsMap =
        existingTopicDetails.values().stream().map(e -> convertToNewTopic(e))
            .collect(Collectors.toMap(e -> e.name(), f -> f));

    assertEquals(expectedTopics.size(), tl.size());

    for (NewTopic topic : expectedTopics) {
      assertTrue(existingTopicNames.contains(topic.name()));
      NewTopic existingTopic = existingTopicDetailsMap.get(topic.name());
      assertEquals(topic.numPartitions(), existingTopic.numPartitions());
      assertEquals(topic.replicationFactor(), existingTopic.replicationFactor());
    }
  }

  protected NewTopic convertToNewTopic(TopicDescription topicDescription) {
    int partitionCount = topicDescription.partitions().size();
    short replicationFactor = (short)topicDescription.partitions().get(0).replicas().size();
    return new NewTopic(topicDescription.name(), partitionCount, replicationFactor);
  }

  protected void loadSourceState(String scenarioFile) throws Exception {
    if (scenarioFile != null) {

      CreateTopicsResult createdTopics = sourceAdminClient.createTopics(loadFromFile(scenarioFile));
      createdTopics.all().get();
    }

  }

  protected void loadTargetState(String scenarioFile) throws Exception {
    if (scenarioFile != null) {
      targetAdminClient.createTopics(loadFromFile(scenarioFile)).all().get();
    }
  }

  protected void saveState(String fileName) {
    Gson gson = new Gson();
    List<NewTopic> topics = new ArrayList<>();
    topics.add(new NewTopic("test1", 1, (short) 1));
    topics.add(new NewTopic("test2", 9, (short) TIMEOUT));

    try {
      FileWriter fileWriter = new FileWriter(fileName);
      gson.toJson(topics, fileWriter);
      fileWriter.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected Collection<NewTopic> loadFromFile(String fileName) throws Exception {
    File testFile = new File(fileName);
    Gson gson = new Gson();
    ClassLoader classloader = this.getClass().getClassLoader();
    InputStream is = null;
    try {
      is = classloader.getResourceAsStream(fileName);
    } catch (Exception e) {
      fail(e.getLocalizedMessage());
    }
    if (is == null) {
      fail("Error loading file " + fileName);
    }
    JsonReader reader = new JsonReader(new InputStreamReader(is));

    NewTopic[] topics = gson.fromJson(reader, NewTopic[].class);
    reader.close();
    is.close();

    return Arrays.asList(topics);
  }

  protected AdminClient getTargetAdminClient() {
    return AdminClient.create(targetCluster.getClientConfig());
  }

  protected AdminClient getSourceAdminClient() {
    return AdminClient.create(sourceCluster.getClientConfig());
  }

  protected TopicManager getTargetTopicManager(String name) {
    Properties clientConfig = targetCluster.getClientConfig();
    clientConfig.setProperty("ignore.topics", "");
    return new TopicManager(name, clientConfig);
  }

  protected RepositoryProviderService getSourceRepo() {
    Properties clientConfig = sourceCluster.getClientConfig();
    clientConfig.setProperty("topicfilter", "");
    RepositoryProviderService result = new KafkaRepositoryProvider();
    result.configure(clientConfig);
    return result;

  }
}
