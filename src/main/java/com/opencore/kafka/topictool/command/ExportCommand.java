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