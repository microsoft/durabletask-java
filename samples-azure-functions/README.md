## Azure Durable Functions setup for Java

In this article, you follow steps to create and run a simple azure durable functions in Java.

### Configure your local environment
- The Java Developer Kit, version 8 or 11. The JAVA_HOME environment variable must be set to the install location of the correct version of the JDK.
- Apache Maven, version 3.0 or above for azure function app creation 
  - If Maven isn't your preferred development tool, check out our similar tutorials to [create a function app](https://docs.microsoft.com/en-us/azure/azure-functions/create-first-function-cli-java?tabs=bash%2Cazure-cli%2Cbrowser)
- [Install Azure Functions Core Tools](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=v4%2Cwindows%2Ccsharp%2Cportal%2Cbash) version 3.0.4585+ or 4.x+


### Prerequisite check
In a terminal or command window, run the following commands to check if the correct versions are installed 
- `java -version`
- `mvn -version`
- `func --version` 

### Use the latest Java worker 
- To create and run durable functions in Java, it is recommended to use the latest java worker jar for V3 and V4
  - V3 - v1.11.0 - [Download]()
  - V4 - v2.2.3  - [Download]()
- Replace the existing core tools java worker with the downloaded version.
	- If your core-tools install location for Windows is at - "C:\Program Files\Microsoft\Azure Functions Core Tools".
	- Replace the `azure-functions-java-worker.jar` at "C:\Program Files\Microsoft\Azure Functions Core Tools\workers\java\"

### Create a simple Azure function 
- Use maven to create a simple Azure function in Java using the below commands
  - `mvn archetype:generate -DarchetypeGroupId=com.microsoft.azure -DarchetypeArtifactId=azure-functions-archetype -DjavaVersion=8`
  - More details to [create a Java function in Azure from the command line](https://docs.microsoft.com/en-us/azure/azure-functions/create-first-function-cli-java?tabs=bash%2Cazure-cli%2Cbrowser)

### Use the Durable SDK and azure functions library for Java
- Update the pom.xml or build.gradle of your project to add the [durabletask-azure-functions](https://mvnrepository.com/artifact/com.microsoft/durabletask-azure-functions) and [durabletask-client](https://mvnrepository.com/artifact/com.microsoft/durabletask-client) as dependencies.


### Durable Functions in Java!
Durable Functions is an extension of Azure Functions that lets you write stateful functions in a serverless compute environment.
More information on terminology and patterns can be found at - [Durable Functions Overview](https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-overview?tabs=csharp)

The following example code shows what the simplest "Hello World" orchestrator function might look like. Note that this example orchestrator doesn't actually orchestrate anything.

```java
@FunctionName("HelloWorldOrchestration")
public String helloWorldOrchestration(
        @DurableOrchestrationTrigger(name = "runtimeState") String runtimeState) {
    return OrchestrationRunner.loadAndRun(runtimeState, ctx -> {
        return String.format("Hello %s!", ctx.getInput(String.class));
    });
}
```

Activity trigger

The activity trigger enables you to author functions that are called by orchestrator functions, known as [activity functions](durable-functions-types-features-overview.md#activity-functions). For Java, the activity trigger is configured using the `@DurableActivityTrigger` annotation.

```java
@FunctionName("SayHello")
public String sayHello(@DurableActivityTrigger(name = "name") String name) {
    return String.format("Hello %s!", name);
}
```


