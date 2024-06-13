package com.microsoft.durabletask.client;

import com.microsoft.durabletask.implementation.protobuf.OrchestratorService;

public enum InstanceIdReuseAction {
  ERROR,
  IGNORE,
  TERMINATE;

  public static OrchestratorService.CreateOrchestrationAction toProtobuf(
      InstanceIdReuseAction action) {
    switch (action) {
      case ERROR:
        return OrchestratorService.CreateOrchestrationAction.ERROR;
      case IGNORE:
        return OrchestratorService.CreateOrchestrationAction.IGNORE;
      case TERMINATE:
        return OrchestratorService.CreateOrchestrationAction.TERMINATE;
      default:
        throw new IllegalArgumentException(String.format("Unknown action value: %s", action));
    }
  }
}
