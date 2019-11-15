package com.opencore.kafka.topictool.command;

public class SyncCommand extends TopicToolCommand{

  public SyncCommand() {
    super();
  }

  public SyncCommand(String sourceRepository, String targetCluster, boolean simulate) {
    super();
    this.setSourceRepository(sourceRepository);
    this.setTargetCluster(targetCluster);
    this.setSimulate(simulate);
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
