// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class DurableTaskGrpcClientFactory {
    private static final ConcurrentMap<Integer, DurableTaskGrpcClient> portToClientMap = new ConcurrentHashMap<>();

    public static DurableTaskClient getClient(int port, String defaultVersion) {
        return portToClientMap.computeIfAbsent(port, DurableTaskGrpcClient::new);
    }
}