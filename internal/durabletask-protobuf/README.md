# Durable Task Protobuf Files

This directory contains the protocol buffer definitions used by the Durable Task Framework Java SDK. The files in this directory are automatically downloaded and updated during the build process from the [microsoft/durabletask-protobuf](https://github.com/microsoft/durabletask-protobuf) repository.

## Directory Structure

- `protos/` - Contains the downloaded proto files
- `PROTO_SOURCE_COMMIT_HASH` - Contains the commit hash of the latest proto file version

## Auto-Update Process

The proto files are automatically downloaded and updated when running Gradle builds. This is handled by the `downloadProtoFiles` task in the `client/build.gradle` file. The task:

1. Downloads the pinned version of `orchestrator_service.proto` specified in `client/build.gradle` (or the `protoBranch` override)
2. Saves the current commit hash for tracking purposes
3. Updates these files before proto compilation begins

The pinned schema includes the dedicated `CloneInstance` RPC from
[microsoft/durabletask-protobuf#82](https://github.com/microsoft/durabletask-protobuf/pull/82).
The options-based Java restart API uses this RPC without falling back to legacy restart or scheduling APIs.
The existing `RestartInstanceRequest` message remains unchanged.

## Manual Update

If you need to manually update the proto files, you can run:

```bash
./gradlew downloadProtoFiles -PprotoBranch=<branch-name>
```