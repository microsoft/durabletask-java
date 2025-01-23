# Durable Task Protobuf Files

This directory contains the protocol buffer definitions used by the Durable Task Framework Java SDK. The files in this directory are automatically downloaded and updated during the build process from the [microsoft/durabletask-protobuf](https://github.com/microsoft/durabletask-protobuf) repository.

## Directory Structure

- `protos/` - Contains the downloaded proto files
- `PROTO_SOURCE_COMMIT_HASH` - Contains the commit hash of the latest proto file version

## Auto-Update Process

The proto files are automatically downloaded and updated when running Gradle builds. This is handled by the `downloadProtoFiles` task in the `client/build.gradle` file. The task:

1. Downloads the latest version of `orchestrator_service.proto`
2. Saves the current commit hash for tracking purposes
3. Updates these files before proto compilation begins

## Manual Update

If you need to manually update the proto files, you can run:

```bash
./gradlew downloadProtoFiles
```

# Durable Task Protobuf Definitions

This repo contains [protocol buffer](https://developers.google.com/protocol-buffers) (protobuf) definitions
used by the Durable Task framework sidecar architecture. It's recommended that Durable Task language SDKs reference
the protobuf contracts in this repo via [Git submodules](https://git-scm.com/book/en/v2/Git-Tools-Submodules).

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
