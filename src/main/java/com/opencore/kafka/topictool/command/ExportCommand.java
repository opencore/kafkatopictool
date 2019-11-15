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

public class ExportCommand extends TopicToolCommand {

  private String filePrefix;
  private String outputFormat;

  public ExportCommand() {
    super();
  }

  public ExportCommand(String sourceCluster, String targetFile, boolean simulate) {
    super();
    this.setSourceCluster(sourceCluster);
    this.setTargetFile(targetFile);
    this.setSimulate(simulate);
  }

  public String getFilePrefix() {
    return filePrefix;
  }

  public void setFilePrefix(String filePrefix) {
    this.filePrefix = filePrefix;
  }

  public ExportCommand(String sourceCluster, String targetFile, boolean simulate,
      String filePrefix) {
    this(sourceCluster, targetFile, simulate);
    this.setFilePrefix(filePrefix);
  }

  public ExportCommand(String sourceCluster, String targetFile, boolean simulate, String filePrefix,
      String outputFormat) {
    this(sourceCluster, targetFile, simulate, filePrefix);
    this.setOutputFormat(outputFormat);
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  @Override
  public boolean checkCommand() {
    return getSourceCluster() != null && getTargetFile() != null;
  }

  @Override
  public int getAction() {
    return Action.EXPORT;
  }
}