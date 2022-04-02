// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import org.junit.jupiter.api.AfterEach;

import java.time.Duration;

public class IntegrationTestBase {
    protected static final Duration defaultTimeout = Duration.ofSeconds(10);

    // All tests that create a server should save it to this variable for proper shutdown
    private DurableTaskGrpcWorker server;

    @AfterEach
    private void shutdown() {
        if (this.server != null) {
            this.server.stop();
        }
    }

    protected TestDurableTaskWorkerBuilder createWorkerBuilder() {
        return new TestDurableTaskWorkerBuilder();
    }

    public class TestDurableTaskWorkerBuilder {
        final DurableTaskGrpcWorker.Builder innerBuilder;

        private TestDurableTaskWorkerBuilder() {
            this.innerBuilder = DurableTaskGrpcWorker.newBuilder();
        }

        public DurableTaskGrpcWorker buildAndStart() {
            DurableTaskGrpcWorker server = this.innerBuilder.build();
            IntegrationTestBase.this.server = server;
            server.start();
            return server;
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
    }
}
