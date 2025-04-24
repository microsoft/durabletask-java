# Contributor Onboarding
Thank you for taking the time to contribute to Durable Functions in Java

## Table of Contents

- [What should I know before I get started?](#what-should-i-know-before-i-get-started)
- [Pre-requisites](#pre-requisites)
- [Pull Request Change Flow](#pull-request-change-flow)
- [Development Setup](#development-setup)
- [Pre Commit Tasks](#pre-commit-tasks)
- [Continuous Integration Guidelines & Conventions](#continuous-integration-guidelines-&-conventions)
- [Getting Help](#getting-help)

## What should I know before I get started
- [Durable Functions Overview](https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-overview)
- [Durable Functions Application Patterns](https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-overview?tabs=in-process%2Cnodejs-v3%2Cv1-model&pivots=java#application-patterns)
- [Azure Functions Java Quickstart](https://learn.microsoft.com/en-us/azure/azure-functions/create-first-function-vs-code-java)

## Pre-requisites
- Gradle 7.4
- Java 17
- Visual Studio or IntelliJ IDEA
- [Azure Functions Core Tools](https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=windows%2Cisolated-process%2Cnode-v4%2Cpython-v2%2Chttp-trigger%2Ccontainer-apps&pivots=programming-language-java) 

## Pull Request Change flow

The general flow for making a change to the library is:

1. 🍴 Fork the repo (add the fork via `git remote add me <clone url here>`
2. 🌳 Create a branch for your change (generally branch from dev) (`git checkout -b my-change`)
3. 🛠 Make your change
4. ✔️ Test your change
5. ⬆️ Push your changes to your fork (`git push me my-change`)
6. 💌 Open a PR to the dev branch
7. 📢 Address feedback and make sure tests pass (yes even if it's an "unrelated" test failure)
8. 📦 [Rebase](https://git-scm.com/docs/git-rebase) your changes into  meaningful commits (`git rebase -i HEAD~N` where `N` is commits you want to squash)
9. :shipit: Rebase and merge (This will be done for you if you don't have contributor access)
10. ✂️ Delete your branch (optional)

