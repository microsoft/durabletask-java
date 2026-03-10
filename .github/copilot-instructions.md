# Copilot Instructions — DurableTask Java SDK

This document provides architectural context for AI assistants working with this codebase.
Focus is on **stable patterns, invariants, and pitfalls** — not file paths or function signatures.

---

## What This Project Is

The **Durable Task SDK for Java** enables writing long-running, stateful workflows as code.
It is the Java implementation of Microsoft's cross-language Durable Task Framework (sibling
SDKs exist in .NET, Python, JavaScript/TypeScript, Go).

This SDK supports both:
- **Azure Durable Functions** (via the Azure Functions integration package)
- **Durable Task Scheduler (DTS)** (via the Azure Managed backend package)

---

## Core Execution Model — Replay-Based Orchestrations

This is the single most important thing to understand. Everything else follows from it.

### How Orchestrations Work

Orchestrations implement `TaskOrchestration` and execute via the `TaskOrchestrationExecutor`.
The runtime replays completed history events to rebuild state, then advances the orchestration
for new work.

On every re-invocation:
1. The executor receives the instance ID, old events, and new events.
2. Completed tasks resolve instantly during replay.
3. Incomplete tasks suspend the orchestration.
4. The resulting actions are sent back to the sidecar.

### The Determinism Rule (Critical)

**Orchestrator code MUST be deterministic.** Every replay must produce the exact same
sequence of actions. Mismatches throw `NonDeterministicOrchestratorException`.

What this means in practice:
- **No `System.currentTimeMillis()`** — use `ctx.getCurrentInstant()`
- **No `Math.random()` or `UUID.randomUUID()`**
- **No direct I/O** (HTTP calls, file reads, database queries) — use activities
- **No non-deterministic control flow** — no data-dependent branching on external state

### Activities vs Orchestrations

Activities are the leaf nodes — they execute side effects exactly once (modulo retries)
and persist results as history events. They can do anything: HTTP calls, DB writes, etc.

**Key mental model:** Orchestrations = coordination logic (deterministic).
Activities = real work (non-deterministic allowed).

---

## Architecture — How Pieces Connect

### Communication Model

The SDK communicates over gRPC to a sidecar (DTS, emulator, or Dapr):

```
Client → gRPC → Sidecar (owns storage & scheduling)
                   ↕
Worker ← gRPC (streaming) ← Sidecar pushes work items
```

### Package Structure

This is a Gradle multi-project build with the following modules:

| Module | Role |
|---|---|
| `client` | Core SDK — orchestration engine, client, worker, gRPC communication |
| `azurefunctions` | Azure Functions (Durable Functions) integration |
| `azuremanaged` | Azure Managed (DTS) backend — connection management & auth |
| `samples` | Standalone sample applications for DTS |
| `samples-azure-functions` | Sample applications for Azure Functions |
| `endtoendtests` | End-to-end integration test suite |

### Protocol Layer

Protobuf definitions live in `internal/durabletask-protobuf/`. Generated Java stubs
provide the gRPC message types.

**Generated proto files should never be manually edited.** They are rebuilt from the proto
definitions.

---

## Build System

- **Build tool:** Gradle (with Gradle Wrapper `./gradlew`)
- **Java version:** JDK 8 for compilation, JDK 11+ for tests
- **Key commands:**
  - `./gradlew build -x test` — Build without tests
  - `./gradlew test` — Run unit tests
  - `./gradlew integrationTest` — Run integration tests (requires DTS emulator)
  - `./gradlew spotbugsMain spotbugsTest` — Run SpotBugs static analysis
  - `./gradlew publishToMavenLocal -PskipSigning` — Publish to local Maven repo

---

## Error Handling Patterns

| Error | When |
|---|---|
| `TaskFailedException` | Activity or sub-orchestration fails (carries `FailureDetails`) |
| `NonDeterministicOrchestratorException` | Replayed action mismatches history |
| `TaskCanceledException` | Wait timeout exceeded or operation canceled |
| `CompositeTaskFailedException` | Multiple parallel tasks failed |

---

## Testing Approach

- **Unit tests:** JUnit-based, in `client/src/test/`
- **Integration tests:** JUnit tests in `client/src/test/` that run against the DTS emulator via the dedicated Gradle `integrationTest` task (uses JUnit tags to select tests)
- **End-to-end tests:** Run against Azure Functions host, in `endtoendtests/`
- **Static analysis:** SpotBugs for bug detection

The DTS emulator is used for integration testing:
```bash
docker run --name durabletask-emulator -p 4001:8080 -d mcr.microsoft.com/dts/dts-emulator:latest
```

---

## Code Conventions

### Naming
- Files: `PascalCase.java`
- Classes: `PascalCase`
- Methods: `camelCase`
- Constants: `UPPER_SNAKE_CASE`
- Packages: `com.microsoft.durabletask`

### License
Every source file starts with the Microsoft copyright header + MIT license reference.

### Builder Pattern
The SDK uses the builder pattern for constructing clients and workers:
- `DurableTaskGrpcClientBuilder` → `DurableTaskGrpcClient`
- `DurableTaskGrpcWorkerBuilder` → `DurableTaskGrpcWorker`

---

## What Not to Touch

- **Generated proto files** — regenerated from proto definitions, never manually edited
- **Proto definitions** in `internal/durabletask-protobuf/` — shared across language SDKs,
  changes must be coordinated

---

## Key Design Constraints to Respect

1. **JDK 8 source compatibility** — the client module targets Java 8
2. **Cross-SDK alignment** — behavior should match .NET/Python/JS/Go SDKs where applicable
3. **No new dependencies** without careful consideration — this is a core SDK
4. **Single-threaded orchestration execution** — correctness depends on sequential replay
