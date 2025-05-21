// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.AfterEach;

import java.time.Duration;

import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerClientOptions;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerWorkerExtensions;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerWorkerOptions;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
public class IntegrationTestBase {
    protected static final Duration defaultTimeout = Duration.ofSeconds(10);

    // All tests that create a server should save it to this variable for proper shutdown
    private DurableTaskGrpcWorker server;    // All tests that create a client are responsible for closing their own gRPC channel
    private ManagedChannel workerChannel;
    private ManagedChannel clientChannel;

    @AfterEach
    private void shutdown() {
        if (this.server != null) {
            this.server.stop();
            this.server = null;
        }

        if (this.workerChannel != null) {
            this.workerChannel.shutdownNow();
            this.workerChannel = null;
        }

        if (this.clientChannel != null) {
            this.clientChannel.shutdownNow();
            this.clientChannel = null;
        }
    }

    protected TestDurableTaskWorkerBuilder createWorkerBuilder() {
        DurableTaskSchedulerWorkerOptions options = new DurableTaskSchedulerWorkerOptions()
            .setEndpointAddress("http://localhost:4001")
            .setTaskHubName("default")
            .setCredential(null)
            .setAllowInsecureCredentials(true);
        Channel grpcChannel = options.createGrpcChannel();
        this.workerChannel = (ManagedChannel) grpcChannel;
        return new TestDurableTaskWorkerBuilder(
            new DurableTaskGrpcWorkerBuilder()
                .grpcChannel(grpcChannel));
    }    
    
    protected DurableTaskGrpcClientBuilder createClientBuilder() {
        DurableTaskSchedulerClientOptions options = new DurableTaskSchedulerClientOptions()
            .setEndpointAddress("http://localhost:4001")
            .setTaskHubName("default")
            .setCredential(null)
            .setAllowInsecureCredentials(true);
        Channel grpcChannel = options.createGrpcChannel();
        // The channel returned is actually a ManagedChannel, so we can safely cast it
        this.clientChannel = (ManagedChannel) grpcChannel;
        return new DurableTaskGrpcClientBuilder()
            .grpcChannel(grpcChannel);
    }

    public class TestDurableTaskWorkerBuilder {
        final DurableTaskGrpcWorkerBuilder innerBuilder;

        private TestDurableTaskWorkerBuilder() {
            this.innerBuilder = new DurableTaskGrpcWorkerBuilder();
        }

        private TestDurableTaskWorkerBuilder(DurableTaskGrpcWorkerBuilder innerBuilder) {
            this.innerBuilder = innerBuilder;
        }

        public DurableTaskGrpcWorker buildAndStart() {
            DurableTaskGrpcWorker server = this.innerBuilder.build();
            IntegrationTestBase.this.server = server;
            server.start();
            return server;
        }

        public TestDurableTaskWorkerBuilder setMaximumTimerInterval(Duration maximumTimerInterval) {
            this.innerBuilder.maximumTimerInterval(maximumTimerInterval);
            return this;
        }

        public TestDurableTaskWorkerBuilder addOrchestrator(
                String name,
                TaskOrchestration implementation) {
            this.innerBuilder.addOrchestration(new TaskOrchestrationFactory() {
                @Override
                public String getName() { return name; }

                @Override
                public TaskOrchestration create() { return implementation; }
            });
            return this;
        }

        public <R> IntegrationTests.TestDurableTaskWorkerBuilder addActivity(
                String name,
                TaskActivity implementation)
        {
            this.innerBuilder.addActivity(new TaskActivityFactory() {
                @Override
                public String getName() { return name; }

                @Override
                public TaskActivity create() { return implementation; }
            });
            return this;
        }

        public TestDurableTaskWorkerBuilder useVersioning(DurableTaskGrpcWorkerVersioningOptions options) {
            this.innerBuilder.useVersioning(options);
            return this;
        }
    }
}
