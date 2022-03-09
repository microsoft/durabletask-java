// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.InvalidProtocolBufferException;
import com.microsoft.durabletask.protobuf.OrchestratorService;

import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.logging.Logger;

// TODO: Move this into the SDK since it shouldn't have any dependencies on Functions anymore
// TODO: JavaDoc
public final class OrchestrationRunner {
    private static final Logger logger = Logger.getLogger(OrchestrationRunner.class.getPackage().getName());

    public static <R> String loadAndRun(
            String triggerStateProtoBase64String,
            OrchestratorFunction<R> orchestratorFunc) {
        byte[] decodedBytes = Base64.getDecoder().decode(triggerStateProtoBase64String);
        byte[] resultBytes = loadAndRun(decodedBytes, orchestratorFunc);
        return Base64.getEncoder().encodeToString(resultBytes);
    }

    public static <R> byte[] loadAndRun(
            byte[] triggerStateProtoBytes,
            OrchestratorFunction<R> orchestratorFunc) {
        if (orchestratorFunc == null) {
            throw new IllegalArgumentException("orchestratorFunc must not be null");
        }

        // Wrap the provided lambda in an anonymous TaskOrchestration
        TaskOrchestration orchestration = ctx -> {
            R output = orchestratorFunc.apply(ctx);
            ctx.complete(output);
        };

        return loadAndRun(triggerStateProtoBytes, orchestration);
    }

    public static <R> String loadAndRun(
            String triggerStateProtoBase64String,
            TaskOrchestration orchestration) {
        byte[] decodedBytes = Base64.getDecoder().decode(triggerStateProtoBase64String);
        byte[] resultBytes = loadAndRun(decodedBytes, orchestration);
        return Base64.getEncoder().encodeToString(resultBytes);
    }

    public static byte[] loadAndRun(byte[] triggerStateProtoBytes, TaskOrchestration orchestration) {
        if (triggerStateProtoBytes == null || triggerStateProtoBytes.length == 0) {
            throw new IllegalArgumentException("triggerStateProtoBytes must not be null or empty");
        }

        if (orchestration == null) {
            throw new IllegalArgumentException("orchestration must not be null");
        }

        OrchestratorService.OrchestratorRequest orchestratorRequest;
        try {
            orchestratorRequest = OrchestratorService.OrchestratorRequest.parseFrom(triggerStateProtoBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("triggerStateProtoBytes was not valid protobuf", e);
        }

        // Register the passed orchestration as the default ("*") orchestration
        var orchestrationFactories = new HashMap<String, TaskOrchestrationFactory>();
        orchestrationFactories.put("*", new TaskOrchestrationFactory() {
            @Override
            public String getName() {
                return "*";
            }

            @Override
            public TaskOrchestration create() {
                return orchestration;
            }
        });

        var taskOrchestrationExecutor = new TaskOrchestrationExecutor(
                orchestrationFactories,
                new JacksonDataConverter(),
                logger);

        // TODO: Error handling
        Collection<OrchestratorService.OrchestratorAction> actions = taskOrchestrationExecutor.execute(
                orchestratorRequest.getPastEventsList(),
                orchestratorRequest.getNewEventsList());

        // TODO: Need to get custom status from executor
        OrchestratorService.OrchestratorResponse response = OrchestratorService.OrchestratorResponse.newBuilder()
                .setInstanceId(orchestratorRequest.getInstanceId())
                .addAllActions(actions)
                .build();

        return response.toByteArray();
    }
}
