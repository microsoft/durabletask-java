// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService;

import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * Helper class for invoking orchestrations directly, without constructing a {@link DurableTaskGrpcWorker} object.
 * <p>
 * This static class can be used to execute orchestration logic directly. In order to use it for this purpose, the
 * caller must provide orchestration state as serialized protobuf bytes.
 * <p>
 * The Azure Functions .NET worker extension is the primary intended user of this class, where orchestration state
 * is provided by trigger bindings.
 */
public final class OrchestrationRunner {
    private static final Logger logger = Logger.getLogger(OrchestrationRunner.class.getPackage().getName());

    private OrchestrationRunner() {
    }

    /**
     * Loads orchestration history from {@code base64EncodedOrchestratorRequest} and uses it to execute the
     * orchestrator function code pointed to by {@code orchestratorFunc}.
     *
     * @param base64EncodedOrchestratorRequest the base64-encoded protobuf payload representing an orchestrator execution request
     * @param orchestratorFunc a function that implements the orchestrator logic
     * @param <R> the type of the orchestrator function output, which must be serializable to JSON
     * @return a base64-encoded protobuf payload of orchestrator actions to be interpreted by the external orchestration engine
     * @throws IllegalArgumentException if either parameter is {@code null} or if {@code base64EncodedOrchestratorRequest} is not valid base64-encoded protobuf
     */
    public static <R> String loadAndRun(
            String base64EncodedOrchestratorRequest,
            OrchestratorFunction<R> orchestratorFunc) {
        // Example string: CiBhOTMyYjdiYWM5MmI0MDM5YjRkMTYxMDIwNzlmYTM1YSIaCP///////////wESCwi254qRBhDk+rgocgAicgj///////////8BEgwIs+eKkQYQzMXjnQMaVwoLSGVsbG9DaXRpZXMSACJGCiBhOTMyYjdiYWM5MmI0MDM5YjRkMTYxMDIwNzlmYTM1YRIiCiA3ODEwOTA2N2Q4Y2Q0ODg1YWU4NjQ0OTNlMmRlMGQ3OA==
        byte[] decodedBytes = Base64.getDecoder().decode(base64EncodedOrchestratorRequest);
        byte[] resultBytes = loadAndRun(decodedBytes, orchestratorFunc);
        return Base64.getEncoder().encodeToString(resultBytes);
    }

    /**
     * Loads orchestration history from {@code orchestratorRequestBytes} and uses it to execute the
     * orchestrator function code pointed to by {@code orchestratorFunc}.
     *
     * @param orchestratorRequestBytes the protobuf payload representing an orchestrator execution request
     * @param orchestratorFunc a function that implements the orchestrator logic
     * @param <R> the type of the orchestrator function output, which must be serializable to JSON
     * @return a protobuf-encoded payload of orchestrator actions to be interpreted by the external orchestration engine
     * @throws IllegalArgumentException if either parameter is {@code null} or if {@code orchestratorRequestBytes} is not valid protobuf
     */
    public static <R> byte[] loadAndRun(
            byte[] orchestratorRequestBytes,
            OrchestratorFunction<R> orchestratorFunc) {
        if (orchestratorFunc == null) {
            throw new IllegalArgumentException("orchestratorFunc must not be null");
        }

        // Wrap the provided lambda in an anonymous TaskOrchestration
        TaskOrchestration orchestration = ctx -> {
            R output = orchestratorFunc.apply(ctx);
            ctx.complete(output);
        };

        return loadAndRun(orchestratorRequestBytes, orchestration);
    }

    /**
     * Loads orchestration history from {@code base64EncodedOrchestratorRequest} and uses it to execute the
     * {@code orchestration}.
     *
     * @param base64EncodedOrchestratorRequest the base64-encoded protobuf payload representing an orchestrator execution request
     * @param orchestration the orchestration to execute
     * @return a base64-encoded protobuf payload of orchestrator actions to be interpreted by the external orchestration engine
     * @throws IllegalArgumentException if either parameter is {@code null} or if {@code base64EncodedOrchestratorRequest} is not valid base64-encoded protobuf
     */
    public static String loadAndRun(
            String base64EncodedOrchestratorRequest,
            TaskOrchestration orchestration) {
        byte[] decodedBytes = Base64.getDecoder().decode(base64EncodedOrchestratorRequest);
        byte[] resultBytes = loadAndRun(decodedBytes, orchestration);
        return Base64.getEncoder().encodeToString(resultBytes);
    }

    /**
     * Loads orchestration history from {@code orchestratorRequestBytes} and uses it to execute the
     * {@code orchestration}.
     *
     * @param orchestratorRequestBytes the protobuf payload representing an orchestrator execution request
     * @param orchestration the orchestration to execute
     * @return a protobuf-encoded payload of orchestrator actions to be interpreted by the external orchestration engine
     * @throws IllegalArgumentException if either parameter is {@code null} or if {@code orchestratorRequestBytes} is not valid protobuf
     */
    public static byte[] loadAndRun(byte[] orchestratorRequestBytes, TaskOrchestration orchestration) {
        if (orchestratorRequestBytes == null || orchestratorRequestBytes.length == 0) {
            throw new IllegalArgumentException("triggerStateProtoBytes must not be null or empty");
        }

        if (orchestration == null) {
            throw new IllegalArgumentException("orchestration must not be null");
        }

        OrchestratorService.OrchestratorRequest orchestratorRequest;
        try {
            orchestratorRequest = OrchestratorService.OrchestratorRequest.parseFrom(orchestratorRequestBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("triggerStateProtoBytes was not valid protobuf", e);
        }

        // Register the passed orchestration as the default ("*") orchestration
        HashMap<String, TaskOrchestrationFactory> orchestrationFactories = new HashMap<>();
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

        TaskOrchestrationExecutor taskOrchestrationExecutor = new TaskOrchestrationExecutor(
                orchestrationFactories,
                new JacksonDataConverter(),
                logger);

        // TODO: Error handling
        TaskOrchestratorResult taskOrchestratorResult = taskOrchestrationExecutor.execute(
                orchestratorRequest.getPastEventsList(),
                orchestratorRequest.getNewEventsList());

        OrchestratorService.OrchestratorResponse response = OrchestratorService.OrchestratorResponse.newBuilder()
                .setInstanceId(orchestratorRequest.getInstanceId())
                .addAllActions(taskOrchestratorResult.getActions())
                .setCustomStatus(StringValue.of(taskOrchestratorResult.getCustomStatus()))
                .build();
        return response.toByteArray();
    }
}
