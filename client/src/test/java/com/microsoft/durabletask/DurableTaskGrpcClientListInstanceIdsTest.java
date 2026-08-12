// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask;

import com.google.protobuf.StringValue;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.ListInstanceIdsRequest;
import com.microsoft.durabletask.implementation.protobuf.OrchestratorService.ListInstanceIdsResponse;
import com.microsoft.durabletask.implementation.protobuf.TaskHubSidecarServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link DurableTaskGrpcClient#listInstanceIds(ListInstanceIdsQuery)} pagination-token handling.
 * <p>
 * These tests stand up an in-process gRPC server whose {@code listInstanceIds} implementation returns a controlled
 * {@code lastInstanceKey}, verifying that the client only surfaces a non-null continuation token when the server
 * returns a present, non-empty key. A present-but-empty key must terminate pagination to avoid an infinite loop.
 */
public class DurableTaskGrpcClientListInstanceIdsTest {

    private Server inProcessServer;
    private ManagedChannel inProcessChannel;

    private DurableTaskClient startClient(StringValue lastInstanceKey, boolean setKey) throws Exception {
        String serverName = InProcessServerBuilder.generateName();
        this.inProcessServer = InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new TaskHubSidecarServiceGrpc.TaskHubSidecarServiceImplBase() {
                @Override
                public void listInstanceIds(
                        ListInstanceIdsRequest request,
                        StreamObserver<ListInstanceIdsResponse> responseObserver) {
                    ListInstanceIdsResponse.Builder builder = ListInstanceIdsResponse.newBuilder()
                        .addAllInstanceIds(Arrays.asList("instance-a", "instance-b"));
                    if (setKey) {
                        builder.setLastInstanceKey(lastInstanceKey);
                    }
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                }
            })
            .build()
            .start();

        this.inProcessChannel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        // The externally-provided channel is not owned by the client, so close() is a no-op; the channel is
        // shut down in tearDown().
        return new DurableTaskGrpcClientBuilder().grpcChannel(this.inProcessChannel).build();
    }

    @AfterEach
    void tearDown() {
        if (this.inProcessChannel != null) {
            this.inProcessChannel.shutdownNow();
        }
        if (this.inProcessServer != null) {
            this.inProcessServer.shutdownNow();
        }
    }

    @Test
    void emptyLastInstanceKeyTerminatesPagination() throws Exception {
        DurableTaskClient client = startClient(StringValue.of(""), true);
        ListInstanceIdsResult result = client.listInstanceIds(new ListInstanceIdsQuery());

        assertEquals(Arrays.asList("instance-a", "instance-b"), result.getInstanceIds());
        assertNull(
            result.getContinuationToken(),
            "A present-but-empty lastInstanceKey must be surfaced as null so callers stop paging.");
    }

    @Test
    void absentLastInstanceKeyTerminatesPagination() throws Exception {
        DurableTaskClient client = startClient(null, false);
        ListInstanceIdsResult result = client.listInstanceIds(new ListInstanceIdsQuery());

        assertNull(result.getContinuationToken());
    }

    @Test
    void nonEmptyLastInstanceKeyIsReturnedAsContinuationToken() throws Exception {
        DurableTaskClient client = startClient(StringValue.of("instance-b"), true);
        ListInstanceIdsResult result = client.listInstanceIds(new ListInstanceIdsQuery());

        assertEquals("instance-b", result.getContinuationToken());
    }
}
