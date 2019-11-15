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

/**
 * A class that represents a command for the TopicTool to execute.
 * Currently implemented commands are:
 *  - sync
 *  - compare
 *  - export
 *
 *  <p>Objects of this class should be created by various clients and
 *  passed into this lib to be executed.
 */
public abstract class TopicToolCommand {
  private String targetCluster;
  private String sourceCluster;
  private String targetFile;
  private String sourceRepository;
  private boolean simulate;

  public String getTargetFile() {
    return targetFile;
  }

  public boolean getSimulate() {
    return simulate;
  }

  public void setSimulate(boolean simulate) {
    this.simulate = simulate;
  }

  public void setTargetFile(String targetFile) {
    this.targetFile = targetFile;
  }

  public String getSourceRepository() {
    return sourceRepository;
  }

  public void setSourceRepository(String sourceRepository) {
    this.sourceRepository = sourceRepository;
  }

  public abstract int getAction();

  public abstract boolean checkCommand();

  public String getTargetCluster() {
    return targetCluster;
  }

  public void setTargetCluster(String targetCluster) {
    this.targetCluster = targetCluster;
  }

  public String getSourceCluster() {
    return sourceCluster;
  }

  public void setSourceCluster(String sourceCluster) {
    this.sourceCluster = sourceCluster;
  }
}
