## placeholder
* Add automatic proto file download and commit hash tracking during build ([#207](https://github.com/microsoft/durabletask-java/pull/207))
* Fix infinite loop when use continueasnew after wait external event ([#183](https://github.com/microsoft/durabletask-java/pull/183))
* Fix the issue "Deserialize Exception got swallowed when use anyOf with external event." ([#185](https://github.com/microsoft/durabletask-java/pull/185))

## v1.5.0
* Fix exception type issue when using `RetriableTask` in fan in/out pattern ([#174](https://github.com/microsoft/durabletask-java/pull/174))
* Add implementation to generate name-based deterministic UUID ([#176](https://github.com/microsoft/durabletask-java/pull/176))
* Update dependencies to resolve CVEs ([#177](https://github.com/microsoft/durabletask-java/pull/177))


## v1.4.0

### Updates
* Refactor `createTimer` to be non-blocking ([#161](https://github.com/microsoft/durabletask-java/pull/161))

## v1.3.0
* Refactor `RetriableTask` and add new `CompoundTask`, fixing Fan-out/Fan-in stuck when using `RetriableTask` ([#157](https://github.com/microsoft/durabletask-java/pull/157))

## v1.2.0

### Updates
* Add `thenAccept` and `thenApply` to `Task` interface ([#148](https://github.com/microsoft/durabletask-java/pull/148))
* Support Suspend and Resume Client APIs ([#151](https://github.com/microsoft/durabletask-java/pull/151))
* Support restartInstance and pass restartPostUri in HttpManagementPayload ([#108](https://github.com/microsoft/durabletask-java/issues/108))
* Improve `TaskOrchestrationContext#continueAsNew` method so it doesn't require `return` statement right after it anymore ([#149](https://github.com/microsoft/durabletask-java/pull/149))

## v1.1.1

### Updates
* Fix exception occurring when invoking the `TaskOrchestrationContext#continueAsNew` method ([#118](https://github.com/microsoft/durabletask-java/issues/118))

## v1.1.0

### Updates
* Fix the potential NPE issue of `DurableTaskClient#terminate` method ([#104](https://github.com/microsoft/durabletask-java/issues/104))
* Add waitForCompletionOrCreateCheckStatusResponse client API ([#115](https://github.com/microsoft/durabletask-java/pull/115))
* Support long timers by breaking up into smaller timers ([#114](https://github.com/microsoft/durabletask-java/issues/114))

## v1.0.0

### New

* Add CHANGELOG.md file to track changes across versions
* Added createHttpManagementPayload API ([#63](https://github.com/microsoft/durabletask-java/issues/63))

### Updates

* update DataConverterException with detail error message ([#78](https://github.com/microsoft/durabletask-java/issues/78))
* update OrchestratorBlockedEvent and TaskFailedException to be unchecked exceptions ([#88](https://github.com/microsoft/durabletask-java/issues/88))
* update dependency azure-functions-java-library to 2.2.0 - include azure-functions-java-spi as `compileOnly` dependency ([#95](https://github.com/microsoft/durabletask-java/pull/95))

### Breaking changes

* Use java worker middleware to avoid wrapper method when create orchestrator function ([#87](https://github.com/microsoft/durabletask-java/pull/87))
* Fixed DurableClientContext.createCheckStatusResponse to return 202 ([#92](https://github.com/microsoft/durabletask-java/pull/92))
* context.allOf() now throws CompositeTaskFailedException(RuntimeException) when one or more tasks fail ([#54](https://github.com/microsoft/durabletask-java/issues/54))
* Updated `DurableTaskClient.purgeInstances(...)` to take a timeout parameter and throw `TimeoutException` ([#37](https://github.com/microsoft/durabletask-java/issues/37))
* The `waitForInstanceStart(...)` and `waitForInstanceCompletion(...)` methods of the `DurableTaskClient` interface now throw a checked `TimeoutException`
