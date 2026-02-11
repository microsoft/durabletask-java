# Plan: Durable Entities for durabletask-java

## Summary

Add Durable Entity support to the durabletask-java SDK, achieving feature parity with
the .NET SDK (microsoft/durabletask-dotnet). Entities are stateful, single-threaded actors
identified by a name+key pair. The sidecar manages scheduling, state persistence, and
message routing; the Java SDK provides the programming model, execution engine, and
client/orchestration integration.

The proto (`orchestrator_service.proto`) already defines **all** entity messages, history
events, RPCs, and work-item formats — no proto changes are required.

---

## Feature Checklist

| # | Feature | Phase | Notes |
|---|---------|-------|-------|
| 1 | `EntityInstanceId` value type (name + key) | 1 | |
| 2 | `TaskEntityState` (get/set/delete state) | 1 | |
| 3 | `TaskEntityContext` (id, signal, startOrchestration) | 1 | |
| 4 | `TaskEntityOperation` (name, input, context, state) | 1 | |
| 5 | `ITaskEntity` interface (`runAsync`) | 1 | |
| 6 | `TaskEntity<TState>` base class with reflection dispatch | 1 | Case-insensitive, parameter binding |
| 7 | State dispatch (dispatch to `TState` methods) | 1 | |
| 8 | Implicit delete (operation name "delete", no method needed) | 1 | |
| 9 | `TaskEntityFactory` functional interface | 1 | |
| 10 | `SignalEntityOptions` (scheduledTime) | 1 | |
| 11 | `CallEntityOptions` (timeout) | 1 | |
| 12 | `EntityOperationFailedException` | 1 | |
| 13 | `TaskEntityExecutor` (batch execution engine) | 2 | Transactional commit/rollback per operation |
| 14 | `OperationResult` → proto `OperationResult` conversion | 2 | Success/failure serialization |
| 15 | `OperationAction` → proto `OperationAction` conversion | 2 | sendSignal, startNewOrchestration |
| 16 | `DurableTaskGrpcWorkerBuilder.addEntity()` | 2 | New `entityFactories` map |
| 17 | Worker `ENTITYREQUEST` work-item handling (v1) | 2 | `EntityBatchRequest` → execute → `CompleteEntityTask` |
| 18 | Worker `ENTITYREQUESTV2` work-item handling | 7 | `EntityRequest` → convert → execute (stretch goal) |
| 19 | Worker entity abandon on failure | 2 | `AbandonTaskEntityWorkItem` RPC |
| 20 | `DurableTaskClient.signalEntity()` | 3 | |
| 21 | `DurableTaskGrpcClient.signalEntity()` | 3 | `SignalEntity` RPC |
| 22 | `DurableTaskClient.getEntityMetadata()` | 3 | |
| 23 | `DurableTaskGrpcClient.getEntityMetadata()` | 3 | `GetEntity` RPC |
| 24 | `EntityMetadata` model class | 3 | |
| 25 | `DurableTaskClient.queryEntities()` | 3 | |
| 26 | `DurableTaskGrpcClient.queryEntities()` | 3 | `QueryEntities` RPC |
| 27 | `EntityQuery` / `EntityQueryResult` model classes | 3 | |
| 28 | `DurableTaskClient.cleanEntityStorage()` | 3 | |
| 29 | `DurableTaskGrpcClient.cleanEntityStorage()` | 3 | `CleanEntityStorage` RPC |
| 30 | `CleanEntityStorageRequest` / `CleanEntityStorageResult` | 3 | |
| 31 | `TaskOrchestrationContext.signalEntity()` | 4 | Fire-and-forget → `SendEntityMessageAction.entityOperationSignaled` |
| 32 | `TaskOrchestrationContext.callEntity()` | 4 | Two-way → `SendEntityMessageAction.entityOperationCalled` + waiter |
| 33 | `TaskOrchestrationContext.lockEntities()` | 4 | Returns `AutoCloseable`; complex lock protocol |
| 34 | `TaskOrchestrationContext.isInCriticalSection()` | 4 | |
| 35 | Handle `ENTITYOPERATIONSIGNALED` history event | 4 | Non-determinism check on replay |
| 36 | Handle `ENTITYOPERATIONCALLED` history event | 4 | Non-determinism check on replay |
| 37 | Handle `ENTITYOPERATIONCOMPLETED` history event | 4 | Deliver result to waiter by requestId |
| 38 | Handle `ENTITYOPERATIONFAILED` history event | 4 | Deliver failure to waiter by requestId |
| 39 | Handle `ENTITYLOCKREQUESTED` history event | 4 | Non-determinism check on replay |
| 40 | Handle `ENTITYLOCKGRANTED` history event | 4 | Deliver to lock waiter by criticalSectionId |
| 41 | Handle `ENTITYUNLOCKSENT` history event | 4 | Non-determinism check on replay |
| 42 | Handle `EVENTSENT` history event (no-op) | 4 | Prevent crash on `default:` branch |
| 43 | `@DurableEntityTrigger` annotation | 5 | |
| 44 | `EntityRunner` (base64 protobuf bridge) | 5 | Analogous to `OrchestrationRunner` |
| 45 | Entity middleware (SPI registration) | 5 | New `META-INF/services/` entry |
| 46 | `DurableClientContext` entity helpers | 5 | `signalEntity`, `getEntityMetadata` |
| 47 | Unit tests (executor, entity model) | 6 | |
| 48 | Integration tests (gRPC worker + sidecar) | 6 | |
| 49 | E2E tests (Azure Functions) | 6 | |
| 50 | Counter entity sample | 7 | |
| 51 | Bank account entity + transfer orchestration sample | 7 | |
| 52 | Extended sessions (entity state caching) | 7 | Optimization, defer |

---

## Phase 1 — Core Entity Model

New package: `com.microsoft.durabletask.entities` inside the `client` module.

### Step 1.1 — `EntityInstanceId`

Create in `client/src/main/java/com/microsoft/durabletask`.

- Immutable value type with `name` (String) and `key` (String) fields.
- `toString()` → `"@{name}@{key}"` (matches .NET `EntityId.ToString()`).
- Static `fromString(String)` factory for parsing.
- Implements `equals`, `hashCode`, `Comparable`.

### Step 1.2 — `TaskEntityState`

- `getState(Class<T>)` → deserializes current state using `DataConverter`.
- `setState(Object)` → serializes and stores.
- `hasState()` / `deleteState()` for existence check and implicit-delete.
- Internal commit/rollback support for transactional semantics:
  - `commit()` — snapshot current state as rollback point.
  - `rollback()` — revert to last committed state.

### Step 1.3 — `TaskEntityOperation`

- `getName()` → operation name (String).
- `getInput(Class<T>)` → deserialize input.
- `hasInput()` → check if input was provided.
- `getContext()` → returns `TaskEntityContext`.
- `getState()` → returns `TaskEntityState`.

### Step 1.4 — `TaskEntityContext`

Abstract class with:
- `getId()` → `EntityInstanceId`.
- `signalEntity(EntityInstanceId, String operationName, Object input, SignalEntityOptions)`.
- `startNewOrchestration(String name, Object input, StartOrchestrationOptions)`.

The concrete implementation (in Phase 2) collects `OperationAction` protos:
- `signalEntity` → `OperationAction.sendSignal` (maps to proto `SendSignalAction`).
- `startNewOrchestration` → `OperationAction.startNewOrchestration` (maps to proto `StartNewOrchestrationAction`).
- Internal commit/rollback for action lists (discard actions on operation failure).

### Step 1.5 — `ITaskEntity` interface

```java
public interface ITaskEntity {
    Object runAsync(TaskEntityOperation operation) throws Exception;
}
```

### Step 1.6 — `TaskEntity<TState>` base class

Implements `ITaskEntity`. On `runAsync`:

1. Retrieve/initialize state via `TaskEntityState.getState(stateType)`.
   If null, call `initializeState()` (default: `stateType.getDeclaredConstructor().newInstance()`).
2. Attempt **reflection dispatch**: find a public method on `this` matching `operation.getName()`
   (case-insensitive via `getClass().getMethods()` scan).
   - Parameter binding: method may accept 0–2 params among `(inputType, TaskEntityContext)`.
   - Return type: void, `T`, or async variants.
3. If no match on `this`, attempt **state dispatch**: find method on `TState` object.
4. If no match, attempt **implicit dispatch**: handle `"delete"` by calling `state.deleteState()`.
5. If still no match, throw `UnsupportedOperationException`.
6. After dispatch, save state back: `state.setState(this.state)`.

### Step 1.7 — Supporting types

- `TaskEntityFactory` — `@FunctionalInterface` returning `ITaskEntity`.
- `SignalEntityOptions` — `scheduledTime` (Instant).
- `CallEntityOptions` — `timeout` (Duration).
- `EntityOperationFailedException` — wraps `FailureDetails` from entity call failures.

---

## Phase 2 — Entity Execution Engine & Worker

### Step 2.1 — `TaskEntityExecutor`

New class in `client/src/main/java/com/microsoft/durabletask`.

Responsibilities:
- Accept `EntityBatchRequest` proto (instanceId, entityState, operations list).
- Look up entity factory by name (parsed from instanceId).
- For each `OperationRequest` in the batch:
  1. Create `TaskEntityOperation` with name, input, context, state.
  2. Call `entity.runAsync(operation)`.
  3. On success: serialize result → `OperationResult.success`, commit state + actions.
  4. On exception: create `OperationResult.failure` with `TaskFailureDetails`, rollback state + actions.
- Return `EntityBatchResult` proto with:
  - `results` — one `OperationResult` per operation.
  - `actions` — accumulated `OperationAction` protos from all committed operations.
  - `entityState` — final serialized state.
  - `completionToken` — passed through from the work item.

### Step 2.2 — `DurableTaskGrpcWorkerBuilder` changes

File: `client/src/main/java/com/microsoft/durabletask/DurableTaskGrpcWorkerBuilder.java`

- Add `Map<String, TaskEntityFactory> entityFactories` field.
- Add `addEntity(String name, TaskEntityFactory factory)` method.
- Pass `entityFactories` to `DurableTaskGrpcWorker` and `TaskEntityExecutor`.

### Step 2.3 — `DurableTaskGrpcWorker` entity work-item handling

File: `client/src/main/java/com/microsoft/durabletask/DurableTaskGrpcWorker.java`

In the work-item processing loop (currently at line 135),
add a new branch after the `ACTIVITYREQUEST` handling:

```
else if (requestType == RequestCase.ENTITYREQUEST) {
    EntityBatchRequest entityRequest = workItem.getEntityRequest();
    // Execute via TaskEntityExecutor
    EntityBatchResult result = taskEntityExecutor.execute(entityRequest);
    // Set completionToken from WorkItem
    EntityBatchResult responseWithToken = result.toBuilder()
        .setCompletionToken(workItem.getCompletionToken())
        .build();
    sidecarClient.completeEntityTask(responseWithToken);
}
```

Error handling: wrap in try/catch, on failure call `sidecarClient.abandonTaskEntityWorkItem(...)`.

Also update `GetWorkItemsRequest` builder to set `maxConcurrentEntityWorkItems`.

### Step 2.4 — `ENTITYREQUESTV2` (deferred to Phase 7)

The v2 format (`EntityRequest`) delivers operations as `HistoryEvent` list instead of
`OperationRequest` list and includes `OperationInfo` with response destinations. This
requires a conversion layer. For now, log a warning and drop v2 work items. The .NET SDK
handles both; Java can add v2 after the core entity feature is stable.

---

## Phase 3 — Client Entity APIs

### Step 3.1 — Model classes

All in `client/src/main/java/com/microsoft/durabletask`:

- **`EntityMetadata`**: `instanceId` (String), `lastModifiedTime` (Instant),
  `backlogQueueSize` (int), `lockedBy` (String, nullable), `serializedState` (String, nullable).
  Add `readStateAs(Class<T>)` using `DataConverter`.

- **`EntityQuery`**: builder pattern with `instanceIdStartsWith`, `lastModifiedFrom/To` (Instant),
  `includeState` (boolean), `includeTransient` (boolean), `pageSize` (Integer),
  `continuationToken` (String).

- **`EntityQueryResult`**: `entities` (List<EntityMetadata>), `continuationToken` (String).

- **`CleanEntityStorageRequest`**: `continuationToken`, `removeEmptyEntities`, `releaseOrphanedLocks`.

- **`CleanEntityStorageResult`**: `continuationToken`, `emptyEntitiesRemoved`, `orphanedLocksReleased`.

### Step 3.2 — `DurableTaskClient` abstract methods

File: `client/src/main/java/com/microsoft/durabletask/DurableTaskClient.java`

Add abstract methods (with convenience overloads):
- `signalEntity(EntityInstanceId id, String operationName, Object input, SignalEntityOptions options)`
- `getEntityMetadata(EntityInstanceId id, boolean includeState)` → `EntityMetadata`
- `queryEntities(EntityQuery query)` → `EntityQueryResult`
- `cleanEntityStorage(CleanEntityStorageRequest request)` → `CleanEntityStorageResult`

### Step 3.3 — `DurableTaskGrpcClient` implementations

File: `client/src/main/java/com/microsoft/durabletask/DurableTaskGrpcClient.java`

Each method maps directly to a proto RPC:

| Method | RPC | Request Proto | Response Proto |
|--------|-----|---------------|----------------|
| `signalEntity` | `SignalEntity` | `SignalEntityRequest` | `SignalEntityResponse` |
| `getEntityMetadata` | `GetEntity` | `GetEntityRequest` | `GetEntityResponse` |
| `queryEntities` | `QueryEntities` | `QueryEntitiesRequest` | `QueryEntitiesResponse` |
| `cleanEntityStorage` | `CleanEntityStorage` | `CleanEntityStorageRequest` | `CleanEntityStorageResponse` |

Note: `SignalEntityRequest` in the proto has `instanceId`, `name`, `input`, `requestId`,
`scheduledTime`. The .NET SDK also sends `requestTime` and `parentTraceContext` but those
fields don't exist in the current proto version — omit for now.

---

## Phase 4 — Orchestration ↔ Entity Integration

This is the most complex phase. All changes are in `TaskOrchestrationExecutor.java`
(`client/src/main/java/com/microsoft/durabletask/TaskOrchestrationExecutor.java`)
and the `TaskOrchestrationContext` interface.

### Step 4.1 — Add entity methods to `TaskOrchestrationContext`

File: `client/src/main/java/com/microsoft/durabletask/TaskOrchestrationContext.java`

```java
// Fire-and-forget signal
void signalEntity(EntityInstanceId id, String operationName, Object input);

// Two-way call with result
<V> Task<V> callEntity(EntityInstanceId id, String operationName, Object input, Class<V> returnType);

// Lock acquisition (returns AutoCloseable to release)
Task<AutoCloseable> lockEntities(List<EntityInstanceId> entityIds);

// Check if currently inside a critical section
boolean isInCriticalSection();
```

### Step 4.2 — Implement `signalEntity()` in executor

Pattern: same as existing `sendEvent()` (line 357).

1. `int id = this.sequenceNumber++`
2. Build `SendEntityMessageAction` with `entityOperationSignaled`:
   - `requestId` = `newUUID().toString()` (deterministic)
   - `operation` = operationName
   - `input` = serialized input
   - `targetInstanceId` = entityInstanceId.toString()
3. Wrap in `OrchestratorAction` with `sendEntityMessage` field → `pendingActions.put(id, action)`
4. No waiter (fire-and-forget).

### Step 4.3 — Implement `callEntity()` in executor

Hybrid pattern combining `callActivity` (action + non-determinism check) and
`waitForExternalEvent` (string-keyed waiter).

1. `int id = this.sequenceNumber++`
2. `String requestId = newUUID().toString()` (deterministic, replayable)
3. Build `SendEntityMessageAction` with `entityOperationCalled`:
   - `requestId` = requestId
   - `operation` = operationName
   - `input` = serialized input
   - `parentInstanceId` = this.instanceId
   - `parentExecutionId` = this.executionId
4. Wrap in `OrchestratorAction` → `pendingActions.put(id, action)`
5. Create `CompletableTask<V>` waiter
6. Add waiter to `outstandingEvents` keyed by **requestId** (not integer taskId)
7. Return the task

Response delivery happens in step 4.5 (event handlers).

### Step 4.4 — Implement `lockEntities()` in executor

This is the most complex sub-feature (~300 lines in .NET). Key mechanics:

1. Sort `entityIds` deterministically (prevents deadlocks).
2. `String criticalSectionId = newUUID().toString()`
3. For each entity in sorted order, at `position` 0..N-1:
   - Build `SendEntityMessageAction` with `entityLockRequested`:
     - `criticalSectionId`, `lockSet` (all entity IDs), `position`, `parentInstanceId`
   - → `pendingActions`
4. Create waiter in `outstandingEvents` keyed by `criticalSectionId`
5. On `entityLockGranted` response, set `isInCriticalSection = true`
6. Return an `AutoCloseable` whose `close()`:
   - For each locked entity: build `SendEntityMessageAction.entityUnlockSent` → `pendingActions`
   - Set `isInCriticalSection = false`

Validation rules:
- Cannot nest lock calls (throw if `isInCriticalSection` is already true).
- `callEntity` within critical section: temporarily release lock awareness, then `recoverLockAfterCall` on response.
- Only entity signals and calls to locked entities are allowed inside critical sections.

### Step 4.5 — Add 7 entity history event handlers

Add to the `processEvent` switch in executor (line 900):

**Action-replay events (non-determinism checks):**

| Event | Handler | Pattern |
|-------|---------|---------|
| `ENTITYOPERATIONSIGNALED` | `handleEntityOperationSignaled` | `pendingActions.remove(eventId)` — if null, throw `NonDeterministicOrchestratorException` |
| `ENTITYOPERATIONCALLED` | `handleEntityOperationCalled` | Same: `pendingActions.remove(eventId)` — non-determinism check |
| `ENTITYLOCKREQUESTED` | `handleEntityLockRequested` | Same: `pendingActions.remove(eventId)` — non-determinism check |
| `ENTITYUNLOCKSENT` | `handleEntityUnlockSent` | Same: `pendingActions.remove(eventId)` — non-determinism check |

**Response events (deliver to waiters):**

| Event | Handler | Key Field | Delivery |
|-------|---------|-----------|----------|
| `ENTITYOPERATIONCOMPLETED` | `handleEntityOperationCompleted` | `requestId` | Deserialize `output`, complete waiter in `outstandingEvents` |
| `ENTITYOPERATIONFAILED` | `handleEntityOperationFailed` | `requestId` | Create `EntityOperationFailedException` from `failureDetails`, complete waiter exceptionally |
| `ENTITYLOCKGRANTED` | `handleEntityLockGranted` | `criticalSectionId` | Complete lock waiter, set `isInCriticalSection = true` |

### Step 4.6 — Handle `EVENTSENT` (no-op)

The existing `EVENTSENT` case is commented out at line 925,
causing a crash on the `default:` branch. Uncomment and make it a no-op:

```java
case EVENTSENT:
    // No action needed — sendEvent is fire-and-forget with no replay validation
    break;
```

This prevents crashes if the sidecar includes `EVENTSENT` events in the history
(which can happen for non-entity `sendEvent()` calls).

### Step 4.7 — New executor state fields

Add to the executor's inner context class:
- `boolean isInCriticalSection = false`
- `String currentCriticalSectionId = null`
- `Set<String> lockedEntityIds` (for validation)

---

## Phase 5 — Azure Functions Integration

### Step 5.1 — `@DurableEntityTrigger` annotation

File: new in `azurefunctions/src/main/java/com/microsoft/durabletask/azurefunctions`

```java
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface DurableEntityTrigger {
    String name();
    String entityName() default "";
}
```

### Step 5.2 — `EntityRunner`

File: new in `client/src/main/java/com/microsoft/durabletask`

Static utility class analogous to `OrchestrationRunner`:

1. Decode base64 string → `EntityBatchRequest` protobuf.
2. Create `TaskEntityExecutor` and execute the batch.
3. Encode `EntityBatchResult` → base64 string.
4. Return the string (Azure Functions host handles the rest).

### Step 5.3 — Entity middleware

File: new class in `azurefunctions/src/main/java/com/microsoft/durabletask/azurefunctions/middleware`

Analogous to the existing `OrchestrationMiddleware`.

Register via SPI: add new entry to
`META-INF/services/com.microsoft.azure.functions.internal.spi.middleware.Middleware`.

### Step 5.4 — `DurableClientContext` entity helpers

File: `azurefunctions/src/main/java/com/microsoft/durabletask/azurefunctions/DurableClientContext.java`

Add convenience methods:
- `signalEntity(EntityInstanceId, String, Object)`
- `getEntityMetadata(EntityInstanceId, boolean)`

These delegate to the underlying `DurableTaskClient`.

---

## Phase 6 — Testing

### Step 6.1 — Unit tests

Location: `client/src/test/java/com/microsoft/durabletask`

Follow the existing pattern in `TaskOrchestrationExecutorTest`
— manually construct `HistoryEvent` protobufs and assert on returned actions.

Test cases:
- **Entity model**: `TaskEntity<TState>` reflection dispatch (case-insensitive method lookup,
  parameter binding, state dispatch, implicit delete, unknown operation → exception).
- **Entity executor**: batch execution with commit/rollback (success → commit, exception →
  rollback state and actions).
- **Orchestration entity events**: for each of the 7 entity history events, verify correct
  pending action removal, non-determinism checks, and waiter delivery.
- **signalEntity**: verify `SendEntityMessageAction.entityOperationSignaled` action produced.
- **callEntity**: verify action produced + waiter registered + response delivered on
  `entityOperationCompleted`/`entityOperationFailed`.
- **lockEntities**: verify sorted lock requests, grant delivery, unlock on close.
- **EVENTSENT**: verify no crash (no-op handler).

### Step 6.2 — Integration tests

Location: `endtoendtests/src/test/java`

Requires sidecar running. Test:
- Register entity + signal from client → verify state via `getEntity`.
- Orchestration calls entity → verify result.
- Query entities, clean entity storage.

### Step 6.3 — E2E tests (Azure Functions)

Location: `samples-azure-functions/src/test/java`

Test `@DurableEntityTrigger` end-to-end with the Functions host.

---

## Phase 7 — Samples & Stretch Goals

### Step 7.1 — Counter entity sample

Location: `samples/src/main/java`

```java
public class CounterEntity extends TaskEntity<Integer> {
    public void add(int amount) { this.state += amount; }
    public void reset() { this.state = 0; }
    public int get() { return this.state; }
    @Override protected Integer initializeState(TaskEntityOperation op) { return 0; }
}
```

### Step 7.2 — Bank account + transfer orchestration sample

- `BankAccountEntity extends TaskEntity<Double>` with `deposit`, `withdraw`, `get`.
- `TransferOrchestration` using `lockEntities` for atomic transfer:
  ```
  lock([source, dest]) → callEntity(source, "withdraw") → callEntity(dest, "deposit")
  ```

### Step 7.3 — `ENTITYREQUESTV2` support

Add conversion from `EntityRequest` (history-based operation list) to
`EntityBatchRequest` (flat operation list) + `OperationInfo` passthrough. Wire into
worker alongside v1 handling.

### Step 7.4 — Extended sessions

In-memory entity state caching between batch invocations (optimization for Azure Functions).
Matches the .NET `ExtendedSessionsCache` pattern. Defer until core feature is stable.

---

## Key Implementation Details

### How `callEntity` works end-to-end

```
Orchestrator code:  Task<V> result = ctx.callEntity(id, "op", input, V.class)
                                ↓
Executor:           1. sequenceNumber++ → action ID
                    2. newUUID() → requestId (deterministic)
                    3. Build OrchestratorAction { sendEntityMessage { entityOperationCalled {
                         requestId, operation, input, parentInstanceId, parentExecutionId } } }
                    4. pendingActions.put(actionId, action)
                    5. outstandingEvents.put(requestId, waiter)
                                ↓
First replay:       Sidecar returns entityOperationCalled history event
                    → handleEntityOperationCalled: pendingActions.remove(eventId) ← non-det check
                                ↓
Response arrives:   Sidecar returns entityOperationCompleted { requestId, output }
                    → handleEntityOperationCompleted: outstandingEvents.get(requestId) → complete(output)
                       OR
                    entityOperationFailed { requestId, failureDetails }
                    → handleEntityOperationFailed: outstandingEvents.get(requestId) → completeExceptionally
```

### How entity batch execution works

```
Sidecar sends:      WorkItem { entityRequest: EntityBatchRequest {
                       instanceId, entityState, operations: [op1, op2, ...] } }
                                ↓
Worker:             taskEntityExecutor.execute(entityBatchRequest)
                                ↓
Executor:           1. Parse entityName from instanceId
                    2. Look up TaskEntityFactory by name
                    3. Create ITaskEntity instance
                    4. Initialize TaskEntityState from entityState
                    5. For each OperationRequest:
                       a. state.commit()
                       b. entity.runAsync(operation) → result or exception
                       c. Success → OperationResult.success, state.commit(), context.commit()
                       d. Exception → OperationResult.failure, state.rollback(), context.rollback()
                    6. Return EntityBatchResult { results, actions, entityState, completionToken }
                                ↓
Worker:             sidecarClient.completeEntityTask(entityBatchResult)
```

### Important: `sendEvent` vs entity actions pattern

The existing `sendEvent()` method (line 357)
puts the action in `pendingActions` but does **not** have a non-determinism check on replay
(there's no handler for `EVENTSENT`). Entity actions **do** have non-determinism checks
because every entity action gets its own specific history event type during replay.

---

## Decisions

- **v1 entity format first**: Implement `ENTITYREQUEST` (v1, `EntityBatchRequest` directly)
  before `ENTITYREQUESTV2` (v2, `EntityRequest` with history-based operations). V2 is a
  stretch goal.
- **No proto changes**: All required entity messages, events, actions, and RPCs are already
  defined. The Java proto stubs are generated and available.
- **gRPC-native entity events**: Unlike the .NET SDK (which has a shim layer converting
  DTFx events), the Java SDK uses proto entity events directly. This means entity operations
  produce `SendEntityMessageAction` and receive entity-specific history events — not
  `EVENTSENT`/`EVENTRAISED` pairs.
- **Transactional per-operation semantics**: Match .NET behavior. Each operation in a batch
  is independently committed or rolled back.
- **Deterministic UUIDs for requestIds**: Use existing `newUUID()` method to generate
  replay-safe request IDs for entity calls and locks.
- **Entity call waiters use `outstandingEvents`**: Keyed by requestId (String), same data
  structure as `waitForExternalEvent`. NOT `openTasks` (which is keyed by integer taskId).
- **Extended sessions deferred**: Optimization that caches entity state across invocations
  in Azure Functions. Not needed for correctness.

---

## Verification

**Build**: `./gradlew build` — must compile and pass all existing tests.

**Unit tests**:
```bash
./gradlew :client:test --tests "*Entity*"
```

**Integration tests** (requires sidecar):
```bash
docker run -d -p 4001:4001 durabletask-sidecar
./gradlew :endtoendtests:test --tests "*Entity*"
```

**Manual smoke test**:
1. Run counter entity sample.
2. Signal entity with "add" operation.
3. Query entity state — verify counter incremented.
4. Run transfer orchestration with locked entities.
5. Verify atomic state changes on both accounts.

**Non-regression**: All existing orchestration + activity tests must continue to pass.
The `EVENTSENT` no-op handler must not break existing `sendEvent` behavior.
