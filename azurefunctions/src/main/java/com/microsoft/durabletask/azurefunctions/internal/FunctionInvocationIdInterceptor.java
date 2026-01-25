// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.durabletask.azurefunctions.internal;

import io.grpc.*;

/**
 * A gRPC client interceptor that adds the Azure Functions invocation ID to outgoing calls
 * for correlation with host-side logs.
 */
public final class FunctionInvocationIdInterceptor implements ClientInterceptor {
    private static final String INVOCATION_ID_METADATA_KEY_NAME = "x-azure-functions-invocationid";
    private static final Metadata.Key<String> INVOCATION_ID_KEY =
            Metadata.Key.of(INVOCATION_ID_METADATA_KEY_NAME, Metadata.ASCII_STRING_MARSHALLER);

    private final String invocationId;

    /**
     * Creates a new interceptor that will add the specified invocation ID to all gRPC calls.
     *
     * @param invocationId the Azure Functions invocation ID to add to calls
     */
    public FunctionInvocationIdInterceptor(String invocationId) {
        this.invocationId = invocationId;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions,
            Channel next) {

        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                if (invocationId != null && !invocationId.isEmpty()) {
                    headers.put(INVOCATION_ID_KEY, invocationId);
                }
                super.start(responseListener, headers);
            }
        };
    }
}
