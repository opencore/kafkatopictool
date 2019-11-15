package com.opencore.kafka.topictool.command;

public class CompareCommand extends TopicToolCommand {

  protected boolean printMismatchOnly;

  public CompareCommand() {
    super();
  }

  public boolean isPrintMismatchOnly() {
    return printMismatchOnly;
  }

  public void setPrintMismatchOnly(boolean printMismatchOnly) {
    this.printMismatchOnly = printMismatchOnly;
  }

  public CompareCommand(String sourceCluster, String targetCluster, boolean simulate) {
    this.setSourceCluster(sourceCluster);
    this.setTargetCluster(targetCluster);
    this.setSimulate(simulate);
  }

  public CompareCommand(String sourceCluster, String targetCluster, boolean simulate, boolean printMismatchOnly) {
    this(sourceCluster, targetCluster, simulate);
    this.setPrintMismatchOnly(printMismatchOnly);
  }

  @Override
  public boolean checkCommand() {
    return getTargetCluster() != null && getSourceCluster() != null;
  }

  @Override
  public int getAction() {
    return Action.COMPARE;
  }
}

