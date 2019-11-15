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
package com.opencore.kafka.topictool.command;

public class SyncCommand extends TopicToolCommand {
  private String topicPattern;

  public SyncCommand() {
    super();
  }

  public SyncCommand(String sourceRepository, String targetCluster, boolean simulate) {
    super();
    this.setSourceRepository(sourceRepository);
    this.setTargetCluster(targetCluster);
    this.setSimulate(simulate);
  }

  public String getTopicPattern() {
    return topicPattern;
  }

  public void setTopicPattern(String topicPattern) {
    this.topicPattern = topicPattern;
  }

  public SyncCommand(String sourceRepository, String targetCluster, boolean simulate, String topicPattern) {
    this(sourceRepository, targetCluster, simulate);
    this.topicPattern = topicPattern;
  }

  @Override
  public int getAction() {
    return Action.SYNC;
  }

  @Override
  public boolean checkCommand() {
    return getSourceRepository() != null && getTargetCluster() != null;
  }
}
