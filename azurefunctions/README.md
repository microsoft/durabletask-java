# Azure Durable Functions setup for Java

In this article, you follow steps to create and run a simple azure durable functions in Java.

## Configure your local environment

The following are the requirements for you local environment:

- The Java Developer Kit, version 8 or 11, is required. Between the two, JDK 11 is recommended.
- The `JAVA_HOME` environment variable must be set to the install location of the correct version of the JDK.
- [Apache Maven](https://maven.apache.org/), version 3.0 or above for azure function app creation, is required for using automatic project creation tools. 
  - If Maven isn't your preferred development tool, check out our similar tutorials to [create a function app](https://docs.microsoft.com/en-us/azure/azure-functions/create-first-function-cli-java?tabs=bash%2Cazure-cli%2Cbrowser). This README also contains instructions for [Gradle](https://gradle.org/).
- [Install Azure Functions Core Tools](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=v4%2Cwindows%2Ccsharp%2Cportal%2Cbash) version 3.0.4585+ or 4.0.4545+


## Prerequisite check
In a terminal or command window, run the following commands to check if the correct versions are installed.

- `java -version`
- `mvn -version`
- `func --version`

## Using the latest Java worker 

At the time of writing, some versions of the Azure Functions Core Tools do not yet have the latest Java worker runtime required for Durable Functions for Java. The following Core Tools *minimum* versions are required:

- v3.0.4585 or greater
- v4.0.4545 or greater

If you aren't able to get a version of the Core Tools in one of these version ranges, you can still use Durable Functions for Java, but you'll need to manually update the version of the Java worker used by your existing Core Tools installation. The following are steps describing how you can update to a compatible version of the Java worker. You can skip these steps if you already have a required Azure Functions Core Tools version installed locally.

### Download the Java Worker for your particular Core Tools version.

The Azure Functions Java worker is a single jar file that can be downloaded locally on your machine using one of the following links.

| Azure Functions Core Tools Version | Java Worker Version |
| - | - |
| v3.x | v1.11.0 ([Download](https://javaworkerrelease.blob.core.windows.net/release/1.11.0/azure-functions-java-worker.jar?sp=r&st=2022-06-01T22:17:09Z&se=2022-12-31T07:17:09Z&spr=https&sv=2020-08-04&sr=b&sig=Lrp0sJ2q91Q2QeNIxAoy4Ulf3ewx1KdNR1td9BXDuxI%3D))
| v4.x | v2.2.3  ([Download](https://javaworkerrelease.blob.core.windows.net/release/2.2.3/azure-functions-java-worker.jar?sp=r&st=2022-06-01T22:19:48Z&se=2022-12-31T07:19:48Z&spr=https&sv=2020-08-04&sr=b&sig=N4gcAP0jVKqAdo59WxZVFDjtfaBiwCJt%2BMBajkv%2FudI%3D))

### Replace the existing core tools Java worker with the downloaded version.

The location of the file to replace is different depending on which operating system you're using:

| OS | Java Worker Location |
| - | - |
| Windows | `"%PROGRAMFILES%\Microsoft\Azure Functions Core Tools"` |
| macOS   | `/usr/local/Cellar/azure-functions-core-tools@4/4.0.4544/workers` |
| Linux   | `/usr/lib/azure-functions-core-tools-<majorVersion>` |

Replace the `azure-functions-java-worker.jar` file from one of the above locations with the downloaded copy.

## Create a new Azure Functions Java app

If you use Maven for building Java apps, then you can use the following `mvn` command to create a new Java Functions app (targeting Java 8):

```bash
mvn archetype:generate -DarchetypeGroupId=com.microsoft.azure -DarchetypeArtifactId=azure-functions-archetype -DjavaVersion=8
```

For more information about creating Azure Function apps using Maven, see the [create a Java function in Azure from the command line](https://docs.microsoft.com/azure/azure-functions/create-first-function-cli-java?tabs=bash%2Cazure-cli%2Cbrowser) documentation.


If you're using Gradle, then we recommend copying the [Azure Functions sample app](/samples-azure-functions/) in this repo as the starting point for your app.

## Use the Durable Task Client SDK and Azure Functions library for Java

To get access to the classes, interfaces, and annotations required to build Durable Functions, update the pom.xml or build.gradle of your project to reference the [durabletask-azure-functions](https://mvnrepository.com/artifact/com.microsoft/durabletask-azure-functions) and [durabletask-client](https://mvnrepository.com/artifact/com.microsoft/durabletask-client) dependencies.

## Add a simple Durable Functions orchestration to your project

Copy the following Java code into a new Java file in your project. It defines three different function types: an HTTP trigger for starting a Durable Functions orchestration, an orchestration trigger function, and an activity trigger function.

```java
package com.functions;

import java.util.Optional;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;

import com.microsoft.durabletask.*;
import com.microsoft.durabletask.azurefunctions.*;

public class DurableFunctionsSample {
    /**
     * This HTTP-triggered function starts the orchestration.
     */
    @FunctionName("StartHelloCities")
    public HttpResponseMessage startHelloCities(
            @HttpTrigger(name = "req", methods = {HttpMethod.POST}) HttpRequestMessage<Optional<String>> req,
            @DurableClientInput(name = "durableContext") DurableClientContext durableContext,
            final ExecutionContext context) {

        DurableTaskClient client = durableContext.getClient();
        String instanceId = client.scheduleNewOrchestrationInstance("HelloCities");
        context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
        return durableContext.createCheckStatusResponse(req, instanceId);
    }

    /**
     * This is the orchestrator function, which can schedule activity functions, create durable timers,
     * or wait for external events in a way that's completely fault-tolerant. The OrchestrationRunner.loadAndRun()
     * static method is used to take the function input and execute the orchestrator logic.
     */
    @FunctionName("HelloCities")
    public String helloCitiesOrchestrator(@DurableOrchestrationTrigger(name = "runtimeState") String runtimeState) {
        return OrchestrationRunner.loadAndRun(runtimeState, ctx -> {
            String result = "";
            result += ctx.callActivity("SayHello", "Tokyo", String.class).await() + ", ";
            result += ctx.callActivity("SayHello", "London", String.class).await() + ", ";
            result += ctx.callActivity("SayHello", "Seattle", String.class).await();
            return result;
        });
    }

    /**
     * This is the activity function that gets invoked by the orchestrator function.
     */
    @FunctionName("SayHello")
    public String sayHello(@DurableActivityTrigger(name = "name") String name) {
        return String.format("Hello %s!", name);
    }
}
```

### Update host.json

Update your host.json file to look similar to the following:

```json
{
  "version": "2.0",
  "logging": {
    "logLevel": {
      "DurableTask.AzureStorage": "Warning",
      "DurableTask.Core": "Warning"
    }
  },
  "extensions": {
    "durableTask": {
      "hubName": "JavaTestHub"
    }
  },
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle.Preview",
    "version": "[4.*, 5.0.0)"
  }
}
```

The important thing to note is the value for `extensionBundle`, which points to the Azure Functions v4 *Preview* bundle. This is the only bundle that currently has the necessary support for Durable Functions for Java.

### Update local.settings.json

Ensure your `local.settings.json` file has a connection string configured for `AzureWebJobsStorage`. If you're using a local storage emulator, like [Azurite](https://docs.microsoft.com/azure/storage/common/storage-use-azurite?tabs=npm), you can use the example below:

```json
{
    "IsEncrypted": false,
    "Values": {
        "AzureWebJobsStorage": "UseDevelopmentStorage=true",
        "FUNCTIONS_WORKER_RUNTIME": "java"
    }
}
```

## Test the Durable Functions orchestration

Use one of the following commands to start your function app locally using the Azure Functions Core Tools:

### Maven

```bash
mvn clean package
mvn azure-functions:run
```

### Gradle

```bash
./gradlew azureFunctionsRun
```

### Running the orchestration

You can use a cURL command from your terminal to test the Durable Functions orchestration.

```bash
curl -i -X POST http://localhost:7071/api/StartHelloCities
```

The output should look something like the following:

```http
HTTP/1.1 201 Created
Content-Type: application/json; charset=utf-8
Date: Thu, 02 Jun 2022 05:43:00 GMT
Server: Kestrel
Location: http://localhost:7071/runtime/webhooks/durabletask/instances/2461b25e-ece5-4e3d-a22e-593e1f47f866?code=yibxD0qEC8MY9hd0UrBR68a8KPkRhuFZAGRkuX5oN5vJ6bLHLjqrhQ==
Transfer-Encoding: chunked

{
  "id": "2461b25e-ece5-4e3d-a22e-593e1f47f866",
  "purgeHistoryDeleteUri": "http://localhost:7071/runtime/webhooks/durabletask/instances/2461b25e-ece5-4e3d-a22e-593e1f47f866?code=yibxD0qEC8MY9hd0UrBR68a8KPkRhuFZAGRkuX5oN5vJ6bLHLjqrhQ==",
  "sendEventPostUri": "http://localhost:7071/runtime/webhooks/durabletask/instances/2461b25e-ece5-4e3d-a22e-593e1f47f866/raiseEvent/{eventName}?code=yibxD0qEC8MY9hd0UrBR68a8KPkRhuFZAGRkuX5oN5vJ6bLHLjqrhQ==",
  "statusQueryGetUri": "http://localhost:7071/runtime/webhooks/durabletask/instances/2461b25e-ece5-4e3d-a22e-593e1f47f866?code=yibxD0qEC8MY9hd0UrBR68a8KPkRhuFZAGRkuX5oN5vJ6bLHLjqrhQ==",
  "terminatePostUri": "http://localhost:7071/runtime/webhooks/durabletask/instances/2461b25e-ece5-4e3d-a22e-593e1f47f866/terminate?reason={text}&code=yibxD0qEC8MY9hd0UrBR68a8KPkRhuFZAGRkuX5oN5vJ6bLHLjqrhQ=="
}
```

To see whether the orchestration completed, send an HTTP GET request to the URL in the `Location` header from the response of the previous command:

```bash
curl -i "http://localhost:7071/runtime/webhooks/durabletask/instances/2461b25e-ece5-4e3d-a22e-593e1f47f866?code=yibxD0qEC8MY9hd0UrBR68a8KPkRhuFZAGRkuX5oN5vJ6bLHLjqrhQ=="
```

If successful, the response should look something like the following:

```http
HTTP/1.1 200 OK
Content-Length: 266
Content-Type: application/json; charset=utf-8
Date: Thu, 02 Jun 2022 05:45:01 GMT
Server: Kestrel

{"name":"HelloCities","instanceId":"2461b25e-ece5-4e3d-a22e-593e1f47f866","runtimeStatus":"Completed","input":null,"customStatus":"","output":"Hello Tokyo!, Hello London!, Hello Seattle!","createdTime":"2022-06-02T05:43:00Z","lastUpdatedTime":"2022-06-02T05:43:00Z"}
```

At this point, you now have a fully function Durable Functions for Java development environment!
