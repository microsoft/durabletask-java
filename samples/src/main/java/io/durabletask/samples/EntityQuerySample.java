// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Sample demonstrating entity querying and storage cleanup.
 * <p>
 * This sample shows how to use the client management APIs:
 * <ul>
 *   <li>{@link DurableEntityClient#queryEntities(EntityQuery)} — query entities with filters and pagination</li>
 *   <li>{@link DurableEntityClient#cleanEntityStorage(CleanEntityStorageRequest)} — remove empty entities
 *       and release orphaned locks</li>
 * </ul>
 * <p>
 * The sample creates several counter entities, queries them by name prefix, then deletes
 * them and cleans up the empty entity storage.
 * <p>
 * Usage: Run this sample with a Durable Task sidecar listening on localhost:4001.
 * <pre>
 *   docker run -d -p 4001:4001 durabletask-sidecar
 *   ./gradlew :samples:run -PmainClass=io.durabletask.samples.EntityQuerySample
 * </pre>
 */
final class EntityQuerySample {

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        // Build the worker with a simple counter entity
        DurableTaskGrpcWorker worker = new DurableTaskGrpcWorkerBuilder()
                .addEntity("Counter", CounterEntity::new)
                .addOrchestration(new TaskOrchestrationFactory() {
                    @Override
                    public String getName() { return "CreateCounters"; }

                    @Override
                    public TaskOrchestration create() {
                        return ctx -> {
                            // Create several counter entities with different keys
                            for (int i = 1; i <= 5; i++) {
                                EntityInstanceId counterId = new EntityInstanceId("Counter", "counter-" + i);
                                ctx.signalEntity(counterId, "add", i * 10);
                            }
                            ctx.complete("Created 5 counters");
                        };
                    }
                })
                .build();

        worker.start();
        System.out.println("Worker started. Counter entity registered.");

        DurableTaskClient client = new DurableTaskGrpcClientBuilder().build();

        // Step 1: Create several counter entities via an orchestration
        String instanceId = client.scheduleNewOrchestrationInstance("CreateCounters");
        client.waitForInstanceCompletion(instanceId, Duration.ofSeconds(30), true);
        System.out.println("Created 5 counter entities.");

        // Step 2: Query all Counter entities (filter by entity name prefix)
        System.out.println("\n--- Querying all Counter entities ---");
        EntityQuery query = new EntityQuery()
                .setInstanceIdStartsWith("Counter")  // filters to @counter prefix
                .setIncludeState(true)
                .setPageSize(3);  // use small page size to demonstrate pagination

        String continuationToken = null;
        int pageNumber = 0;
        do {
            if (continuationToken != null) {
                query.setContinuationToken(continuationToken);
            }

            EntityQueryResult result = client.getEntities().queryEntities(query);
            pageNumber++;
            System.out.printf("Page %d: %d entities%n", pageNumber, result.getEntities().size());

            for (EntityMetadata entity : result.getEntities()) {
                EntityInstanceId entityId = entity.getEntityInstanceId();
                Integer state = entity.readStateAs(Integer.class);
                System.out.printf("  %s/%s = %d (lastModified: %s)%n",
                        entityId.getName(), entityId.getKey(), state, entity.getLastModifiedTime());
            }

            continuationToken = result.getContinuationToken();
        } while (continuationToken != null);

        // Step 3: Delete all counter entities by signaling the implicit "delete" operation
        System.out.println("\n--- Deleting all counter entities ---");
        for (int i = 1; i <= 5; i++) {
            EntityInstanceId counterId = new EntityInstanceId("Counter", "counter-" + i);
            client.getEntities().signalEntity(counterId, "delete");
        }
        // Give time for delete signals to be processed
        Thread.sleep(3000);

        // Step 4: Clean entity storage to remove the now-empty entities
        System.out.println("\n--- Cleaning entity storage ---");
        CleanEntityStorageRequest cleanRequest = new CleanEntityStorageRequest()
                .setRemoveEmptyEntities(true)
                .setReleaseOrphanedLocks(true);

        CleanEntityStorageResult cleanResult = client.getEntities().cleanEntityStorage(cleanRequest);
        System.out.printf("Cleaned storage: %d empty entities removed, %d orphaned locks released%n",
                cleanResult.getEmptyEntitiesRemoved(),
                cleanResult.getOrphanedLocksReleased());

        // Step 5: Verify entities are gone
        EntityQueryResult afterClean = client.getEntities().queryEntities(
                new EntityQuery().setInstanceIdStartsWith("Counter"));
        System.out.printf("Entities remaining after cleanup: %d%n", afterClean.getEntities().size());

        worker.stop();
    }

    /**
     * A simple counter entity (reused for this sample).
     */
    public static class CounterEntity extends AbstractTaskEntity<Integer> {
        public void add(int amount) { this.state += amount; }
        public int get() { return this.state; }

        @Override
        protected Integer initializeState(TaskEntityOperation operation) { return 0; }

        @Override
        protected Class<Integer> getStateType() { return Integer.class; }
    }
}
