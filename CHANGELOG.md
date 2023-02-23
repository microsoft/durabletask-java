## v1.1.0

### Updates
* Support Suspend and Resume Client APIs ([#104](https://github.com/microsoft/durabletask-java/issues/104))
* Fix the potential NPE issue of `DurableTaskClient terminate` method ([#104](https://github.com/microsoft/durabletask-java/issues/104))
* Add waitForCompletionOrCreateCheckStatusResponse client API ([#108](https://github.com/microsoft/durabletask-java/pull/108))


## v1.0.0

### New

* Add CHANGELOG.md file to track changes across versions

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
