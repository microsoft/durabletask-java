# Contributor Onboarding
This contributor guide explains how to make and test changes to Durable Functions in Java.
Thank you for taking the time to contribute to the DurableTask Java SDK!

## Table of Contents

- [Relevant Docs](#relevant-docs)
- [Prerequisites](#prerequisites)
- [Pull Request Change Flow](#pull-request-change-flow)
- [Testing with a Durable Functions app](#testing-with-a-durable-functions-app)
- [Debugging .NET packages from a Durable Functions Java app](#debugging-net-packages-from-a-durable-functions-java-app)

## Relevant Docs
- [Durable Functions Overview](https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-overview)
- [Durable Functions Application Patterns](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-overview?tabs=in-process%2Cnodejs-v3%2Cv1-model&pivots=java#application-patterns)
- [Azure Functions Java Quickstart](https://learn.microsoft.com/en-us/azure/azure-functions/create-first-function-vs-code-java)

## Prerequisites
- Visual Studio Code
- [Azure Functions Core Tools](https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=windows%2Cisolated-process%2Cnode-v4%2Cpython-v2%2Chttp-trigger%2Ccontainer-apps&pivots=programming-language-java)
- Apache Maven 3.8.1 or higher (Note: the instructions in this doc were validated using Apache Maven 3.9.9)
- Gradle 7.4
- Java 8 or higher (Note: the instructions in this doc were validated using Java 17)

## Pull Request Change Flow

The general flow for making a change to the library is:

1. 🍴 Fork the repo (add the fork via `git remote add me <clone url here>`)
2. 🌳 Create a branch for your change (generally branch from dev) (`git checkout -b my-change`)
3. 🛠 Make your change
4. ✔️ Test your change
5. ⬆️ Push your changes to your fork (`git push me my-change`)
6. 💌 Open a PR to the dev branch
7. 📢 Address feedback and make sure tests pass (yes even if it's an "unrelated" test failure)
8. 📦 [Rebase](https://git-scm.com/docs/git-rebase) your changes into meaningful commits (`git rebase -i HEAD~N` where `N` is commits you want to squash)
9. :shipit: Rebase and merge (This will be done for you if you don't have contributor access)
10. ✂️ Delete your branch (optional)

## Testing with a Durable Functions app

The following instructions explain how to test durabletask-java changes in a Durable Functions Java app.

1. After making changes in durabletask-java, you will need to increment the version number in build.gradle. For example, if you make a change in the azurefunctions directory, then you would update the version in `azurefunctions/build.gradle`.
2. In the durabletask-java repo, from the root of the project, run `gradle clean build`. This will create the .jar files with the updated version that you specified.
3. To get the .jar file that was created, go to the `build/libs` directory. For example, if you made a change in azurefunctions, then go to `durabletask-java/azurefunctions/build/libs`. If you made a change to client, then go to `durabletask-java/client/build/libs`. Add the .jar files that you are testing to a local directory.
4. [Create a Durable Functions Java app](https://learn.microsoft.com/en-us/azure/azure-functions/durable/quickstart-java?tabs=bash&pivots=create-option-vscode) if you haven't done so already.
5. In the Durable Functions Java app, run the following command to install the local .jar files that were created in step 2: `mvn install:install-file -Dfile="<path to .jar file that was created in step 2>" -DgroupId="com.microsoft" -DartifactId="<name of .jar file>" -Dversion="<version>" -Dpackaging="jar" -DlocalRepositoryPath="<path to Durable Functions Java app>"`.

For example, if you created custom `durabletask-client` and `durabletask-azure-functions` packages with version 1.6.0 in step 2, then you would run the following commands:

```
mvn install:install-file -Dfile="C:/Temp/durabletask-client-1.6.0.jar" -DgroupId="com.microsoft" -DartifactId="durabletask-client" -Dversion="1.6.0" -Dpackaging="jar" -DlocalRepositoryPath="C:/df-java-sample-app"

mvn install:install-file -Dfile="C:/Temp/durabletask-azure-functions-1.6.0.jar" -DgroupId="com.microsoft" -DartifactId="durabletask-azure-functions" -Dversion="1.6.0" -Dpackaging="jar" -DlocalRepositoryPath="C:/df-java-sample-app"
```

6. Run `mvn clean package` from the Durable Functions app root folder.
7. Run `mvn azure-functions:run` from the Durable Functions app root folder.

## Debugging .NET packages from a Durable Functions Java app

If you want to debug into the Durable Task or any of the .NET bits, follow the instructions below:

1. If you would like to debug a custom local WebJobs extension package then create the custom package, place it in a local directory, and then run `func extensions install --package Microsoft.Azure.WebJobs.Extensions.DurableTask --version <VERSION>`. If you update the version while debugging and the new version doesn't get picked up, then try running `func extensions install` to get the new changes.
2. Make sure the Durable Functions Java debugging is set up already and the debugger has started the `func` process.
3. In the VSCode editor for DurableTask, click Debug -> .NET Core Attach Process, search for `func host start` process and attach to it.
4. Add a breakpoint in both editors and continue debugging.
