// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.azureblobpayloads;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.microsoft.durabletask.DurableTaskGrpcWorker;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.OrchestrationMetadata;
import com.microsoft.durabletask.TaskActivity;
import com.microsoft.durabletask.TaskActivityFactory;
import com.microsoft.durabletask.history.ExecutionStartedEvent;
import com.microsoft.durabletask.history.GenericEvent;
import com.microsoft.durabletask.history.TaskCompletedEvent;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.*;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RootContainerRoundTripTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void rootTokens_resolveThroughClientWorkerAndStreamedHistory(boolean compressed) throws Exception {
        OfflineBlobService service = new OfflineBlobService();
        BlobPayloadStore store = service.createStore("$root", compressed);
        LargePayloadStorageOptions options = new LargePayloadStorageOptions().setThresholdBytes(1024);
        String payload = "root payload \u00e9\n".repeat(200);
        String literal = "blob:v1:$root:not-a-guid";
        AtomicReference<CreateInstanceRequest> scheduled = new AtomicReference<>();
        AtomicReference<ActivityResponse> completion = new AtomicReference<>();
        AtomicReference<String> activityInput = new AtomicReference<>();
        CountDownLatch completed = new CountDownLatch(1);
        String serverName = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(serverName).directExecutor()
            .addService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
                @Override
                public void startInstance(CreateInstanceRequest request,
                        StreamObserver<CreateInstanceResponse> observer) {
                    scheduled.set(request);
                    observer.onNext(CreateInstanceResponse.newBuilder().setInstanceId(request.getInstanceId()).build());
                    observer.onCompleted();
                }

                @Override
                public void getWorkItems(GetWorkItemsRequest request, StreamObserver<WorkItem> observer) {
                    observer.onNext(WorkItem.newBuilder().setCompletionToken("activity-token")
                        .setActivityRequest(ActivityRequest.newBuilder().setName("Echo").setTaskId(1)
                            .setOrchestrationInstance(OrchestrationInstance.newBuilder().setInstanceId("root-test"))
                            .setInput(scheduled.get().getInput())).build());
                    // Keep the stream open until worker shutdown, as a scheduler would.
                }

                @Override
                public void completeActivityTask(ActivityResponse request, StreamObserver<CompleteTaskResponse> observer) {
                    completion.set(request);
                    observer.onNext(CompleteTaskResponse.getDefaultInstance());
                    observer.onCompleted();
                    completed.countDown();
                }

                @Override
                public void getInstance(GetInstanceRequest request, StreamObserver<GetInstanceResponse> observer) {
                    observer.onNext(GetInstanceResponse.newBuilder().setExists(true)
                        .setOrchestrationState(OrchestrationState.newBuilder().setInstanceId("root-test")
                            .setName("Root").setInput(scheduled.get().getInput()).setOutput(completion.get().getResult())
                            .setOrchestrationStatus(OrchestrationStatus.ORCHESTRATION_STATUS_COMPLETED)).build());
                    observer.onCompleted();
                }

                @Override
                public void streamInstanceHistory(StreamInstanceHistoryRequest request,
                        StreamObserver<HistoryChunk> observer) {
                    observer.onNext(HistoryChunk.newBuilder()
                        .addEvents(HistoryEvent.newBuilder().setExecutionStarted(
                            com.microsoft.durabletask.implementation.protobuf.OrchestratorService.ExecutionStartedEvent
                                .newBuilder().setName("Root").setInput(scheduled.get().getInput())))
                        .addEvents(HistoryEvent.newBuilder().setTaskCompleted(
                            com.microsoft.durabletask.implementation.protobuf.OrchestratorService.TaskCompletedEvent
                                .newBuilder().setTaskScheduledId(1).setResult(completion.get().getResult())))
                        .addEvents(HistoryEvent.newBuilder().setGenericEvent(
                            com.microsoft.durabletask.implementation.protobuf.OrchestratorService.GenericEvent
                                .newBuilder().setData(StringValue.of(literal))))
                        .build());
                    observer.onCompleted();
                }
            }).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        DurableTaskGrpcClientBuilder clientBuilder = new DurableTaskGrpcClientBuilder().grpcChannel(channel);
        LargePayloadClientExtensions.useExternalizedPayloads(clientBuilder, store, options);
        DurableTaskGrpcWorkerBuilder workerBuilder = new DurableTaskGrpcWorkerBuilder().grpcChannel(channel)
            .addActivity(new TaskActivityFactory() {
                @Override
                public String getName() {
                    return "Echo";
                }

                @Override
                public TaskActivity create() {
                    return ctx -> {
                        String input = ctx.getInput(String.class);
                        activityInput.set(input);
                        return input;
                    };
                }
            });
        LargePayloadWorkerExtensions.useExternalizedPayloads(workerBuilder, store, options);
        try (DurableTaskClient client = clientBuilder.build();
             DurableTaskGrpcWorker worker = workerBuilder.build()) {
            client.scheduleNewOrchestrationInstance("Root", payload);
            String inputToken = scheduled.get().getInput().getValue();
            assertTrue(inputToken.startsWith("blob:v1:$root:"));
            assertTrue(store.isKnownPayloadToken(inputToken));
            worker.start();
            assertTrue(completed.await(10, TimeUnit.SECONDS), "Worker must complete the activity");
            assertFalse(completion.get().hasFailureDetails());
            assertEquals(payload, activityInput.get(), "Worker typed input must contain the original payload");
            assertTrue(store.isKnownPayloadToken(completion.get().getResult().getValue()));
            OrchestrationMetadata metadata = client.getInstanceMetadata("root-test", true);
            assertEquals(payload, metadata.readInputAs(String.class));
            assertEquals(payload, metadata.readOutputAs(String.class));
            int downloadsBeforeHistory = service.downloads;
            List<com.microsoft.durabletask.history.HistoryEvent> history = client.getOrchestrationHistory("root-test");
            assertEquals(3, history.size());
            assertEquals(scheduledPayload(payload), ((ExecutionStartedEvent) history.get(0)).getInput());
            assertEquals(scheduledPayload(payload), ((TaskCompletedEvent) history.get(1)).getResult());
            assertEquals(literal, ((GenericEvent) history.get(2)).getData());
            assertEquals(downloadsBeforeHistory + 2, service.downloads,
                "A token-looking literal must not cause a blob download");
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
            assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static String scheduledPayload(String payload) {
        return new com.microsoft.durabletask.JacksonDataConverter().serialize(payload);
    }
}
