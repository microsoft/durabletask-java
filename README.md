# Durable Task Client SDK for Java

[![Build](https://github.com/microsoft/durabletask-java/actions/workflows/build-validation.yml/badge.svg)](https://github.com/microsoft/durabletask-java/actions/workflows/build-validation.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

This repo contains the Java SDK for the Durable Task Framework as well as classes and annotations to support running [Azure Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable/durable-functions-overview?tabs=java) for Java. With this SDK, you can define, schedule, and manage durable orchestrations using ordinary Java code.

### Simple, fault-tolerant sequences

```java
// *** Simple, fault-tolerant, sequential orchestration ***
String result = "";
result += ctx.callActivity("SayHello", "Tokyo", String.class).await() + ", ";
result += ctx.callActivity("SayHello", "London", String.class).await() + ", ";
result += ctx.callActivity("SayHello", "Seattle", String.class).await();
return result;
```

### Replay-safe orchestration logging

Orchestrator code re-executes while rebuilding state from history. Wrap an existing
`java.util.logging.Logger` to suppress log output during those replay segments:

```java
Logger logger = ctx.createReplaySafeLogger(
    Logger.getLogger(MyOrchestration.class.getName()));

logger.info(() -> "Starting orchestration " + ctx.getInstanceId());
String result = ctx.callActivity("ProcessItem", input, String.class).await();
logger.info(() -> "Activity returned: " + result);
```

In Azure Functions, pass `ExecutionContext.getLogger()` instead of creating a named
logger so the output retains its invocation ID and normal host routing:

```java
Logger logger = ctx.createReplaySafeLogger(executionContext.getLogger());
```

Replay-safe logging suppresses calls made while replaying; it does not guarantee
exactly-once log delivery across failed or retried live orchestration turns.

### Reliable fan-out / fan-in orchestration pattern

```java
// Get the list of work-items to process
List<?> batch = ctx.callActivity("GetWorkBatch", List.class).await();

// Schedule each task to run in parallel
List<Task<Integer>> parallelTasks = batch.stream()
        .map(item -> ctx.callActivity("ProcessItem", item, Integer.class))
        .collect(Collectors.toList());

// Wait for all tasks to complete, then return the aggregated sum of the results
List<Integer> results = ctx.allOf(parallelTasks).await();
return results.stream().reduce(0, Integer::sum);
```

### Long-running human interaction pattern (approval workflow)

```java
ApprovalInfo approvalInfo = ctx.getInput(ApprovalInfo.class);
ctx.callActivity("RequestApproval", approvalInfo).await();

Duration timeout = Duration.ofHours(72);
try {
    // Wait for an approval. A TaskCanceledException will be thrown if the timeout expires.
    boolean approved = ctx.waitForExternalEvent("ApprovalEvent", timeout, boolean.class).await();
    approvalInfo.setApproved(approved);

    ctx.callActivity("ProcessApproval", approvalInfo).await();
} catch (TaskCanceledException timeoutEx) {
    ctx.callActivity("Escalate", approvalInfo).await();
}
```

### Eternal monitoring orchestration

```java
JobInfo jobInfo = ctx.getInput(JobInfo.class);
String jobId = jobInfo.getJobId();

String status = ctx.callActivity("GetJobStatus", jobId, String.class).await();
if (status.equals("Completed")) {
    // The job is done - we can exit now
    ctx.callActivity("SendAlert", jobId).await();
} else {
    // wait N minutes before doing the next poll
    Duration pollingDelay = jobInfo.getPollingDelay();
    ctx.createTimer(pollingDelay).await();

    // restart from the beginning
    ctx.continueAsNew(jobInfo);
}

return null;
```

## Maven Central packages

The following packages are produced from this repo.

| Package | Latest version |
| - | - |
| Durable Task - Client | [![Maven Central](https://img.shields.io/maven-central/v/com.microsoft/durabletask-client?label=durabletask-client)](https://mvnrepository.com/artifact/com.microsoft/durabletask-client/1.0.0) |
| Durable Task - Azure Functions | [![Maven Central](https://img.shields.io/maven-central/v/com.microsoft/durabletask-azure-functions?label=durabletask-azure-functions)](https://mvnrepository.com/artifact/com.microsoft/durabletask-azure-functions/1.0.1) |

## Azure Durable Task Scheduler authentication

The `com.microsoft:durabletask-azuremanaged` package configures clients and workers
through `DurableTaskSchedulerClientOptions`, `DurableTaskSchedulerWorkerOptions`,
or the corresponding `DurableTaskSchedulerClientExtensions` and
`DurableTaskSchedulerWorkerExtensions` convenience methods.

### Token audience

Set `resourceId` using `setResourceId(...)` on either options class, the optional
last argument of the `createClientBuilder`, `createWorkerBuilder`, and
`useDurableTaskScheduler` overloads, or `ResourceId` in a connection string.
This is a **token audience URI**, not an Azure Resource Manager resource path.
Existing overloads remain supported.

| Configuration | Selected audience |
| --- | --- |
| Explicit nonempty `resourceId` / `ResourceId` | The normalized explicit value |
| Missing, null, or empty value, with `REGION_NAME` starting with `usgov` or `usdod` (case-insensitive) | `https://durabletask.azure.us` |
| All other cases | `https://durabletask.io` |

**Default behavior change:** applications running in US Government or DoD regions
now select the government audience when no explicit audience is provided.
Set `ResourceId=https://durabletask.io` to retain the public audience in those
regions. Prefixes, not substrings, are matched: `chinaeast2`, `notusgov`, and
`notusdod` still use the public default. No audience is inferred from the endpoint.

Explicit values have surrounding whitespace (including Unicode whitespace such as
em spaces and non-breaking spaces) and trailing `/` characters removed,
then one existing `/.default` suffix removed case-insensitively, followed by any
remaining trailing `/` characters. URI casing is otherwise preserved. For example,
`https://durabletask.azure.us//.DEFAULT//` requests
`https://durabletask.azure.us/.default`, and `api://CustomAudience/resource/.DEFAULT/`
requests `api://CustomAudience/resource/.default`. Whitespace-only input, `///`,
`/.default`, and `/.DEFAULT///` throw `IllegalArgumentException`; use an omitted
or genuinely empty value for the default.

Defaults are resolved per options instance or parsed connection string, rather
than at class initialization. Setting a null or empty audience explicitly resolves
the default again at that point. The selected audience is retained when creating
channels, refreshing tokens, and reconnecting. Connection-string conversion does
not normalize the audience again.

### Government-cloud example and credential authority

The **audience**, **credential authority/cloud**, and **service endpoint** are
independent settings. Neither `resourceId` nor `REGION_NAME` changes the endpoint
or credential authority. For an already-created `TokenCredential`, configure
authority on that credential; token requests do not override it.

```java
import com.azure.core.credential.TokenCredential;
import com.azure.identity.AzureAuthorityHosts;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.microsoft.durabletask.DurableTaskGrpcClientBuilder;
import com.microsoft.durabletask.DurableTaskGrpcWorkerBuilder;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerClientExtensions;
import com.microsoft.durabletask.azuremanaged.DurableTaskSchedulerWorkerExtensions;

// Set these to your scheduler's actual endpoint and task hub.
String endpoint = System.getenv("DTS_ENDPOINT");
String taskHub = System.getenv("DTS_TASK_HUB");
TokenCredential credential = new DefaultAzureCredentialBuilder()
    .authorityHost(AzureAuthorityHosts.AZURE_GOVERNMENT)
    .build();

DurableTaskGrpcClientBuilder clientBuilder =
    DurableTaskSchedulerClientExtensions.createClientBuilder(
        endpoint, taskHub, credential, "https://durabletask.azure.us");
DurableTaskGrpcWorkerBuilder workerBuilder =
    DurableTaskSchedulerWorkerExtensions.createWorkerBuilder(
        endpoint, taskHub, credential, "https://durabletask.azure.us");
```

When the SDK constructs the credential from a connection string, use the optional
`AuthorityHost` property:

```text
Endpoint=<your-scheduler-endpoint>;TaskHub=<your-task-hub>;Authentication=DefaultAzure;ResourceId=https://durabletask.azure.us;AuthorityHost=https://login.microsoftonline.us/
```

`AuthorityHost` is forwarded to Azure Identity for `DefaultAzure`, `Environment`,
`WorkloadIdentity`, and `InteractiveBrowser` authentication. Omission or an empty
value leaves Azure Identity's defaults intact, including `AZURE_AUTHORITY_HOST`
where supported. It is not an authority override on client or worker options.

Managed identity uses the hosting environment's identity endpoint; an Entra
authority override does not apply. Developer-tool credentials (`AzureCli`,
`AzurePowerShell`, `VisualStudioCode`, and `IntelliJ`) use those tools' cloud
configuration, not the connection string's `AuthorityHost`. Configure them
separately, including when they are used by `DefaultAzureCredential` (for example,
`az cloud set --name AzureUSGovernment` before signing in with Azure CLI).
`Authentication=None` remains anonymous.

## Getting started with Azure Functions

For information about how to get started with Durable Functions for Java, see the [Azure Functions README.md](/azurefunctions/README.md) content.

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
