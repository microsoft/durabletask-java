# Durable Task Client SDK for Java

[![Build](https://github.com/microsoft/durabletask-java/actions/workflows/build-validation.yml/badge.svg)](https://github.com/microsoft/durabletask-java/actions/workflows/build-validation.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

⚠ This project is preview quality and not yet ready for production use ⚠

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
| Durable Task - Client | ![Maven Central](https://img.shields.io/maven-central/v/com.microsoft/durabletask-client?label=durabletask-client) |
| Durable Task - Azure Functions | ![Maven Central](https://img.shields.io/maven-central/v/com.microsoft/durabletask-azure-functions?label=durabletask-azure-functions) |

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