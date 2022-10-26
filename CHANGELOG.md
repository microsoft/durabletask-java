## v1.0.0

### New

* Add CHANGELOG.md file to track changes across versions
* context.allOf() throws CompositeTaskFailedException(RuntimeException) when one or more tasks fail ([#54](https://github.com/microsoft/durabletask-java/issues/54))


### Updates

* Updated package version to v1.0.0 - to be updated
* update DataConverterException with detail error message ([#78](https://github.com/microsoft/durabletask-java/issues/78))
* update OrchestratorBlockedEvent and TaskFailedException to be unchecked exceptions ([#88](https://github.com/microsoft/durabletask-java/issues/88))
* updated PurgeInstances to take a timeout parameter and throw TimeoutException ([#37](https://github.com/microsoft/durabletask-java/issues/37))

### Breaking changes

* Use java worker middleware to avoid wrapper method when create orchestrator function ([#87](https://github.com/microsoft/durabletask-java/pull/87))
* Fixed DurableClientContext.createCheckStatusResponse to return 202 ([#92](https://github.com/microsoft/durabletask-java/pull/92))

* to be updated