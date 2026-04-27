/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.durabletask.azurefunctions.internal.middleware;

import com.microsoft.azure.functions.internal.spi.middleware.Middleware;
import com.microsoft.azure.functions.internal.spi.middleware.MiddlewareChain;
import com.microsoft.azure.functions.internal.spi.middleware.MiddlewareContext;
import com.microsoft.durabletask.DataConverter;

/**
 * Durable Function Entity Middleware
 *
 * <p>This class is internal and is hence not for public use. Its APIs are unstable and can change
 * at any time.
 */
public class EntityMiddleware implements Middleware {

    private static final String ENTITY_TRIGGER = "DurableEntityTrigger";

    @Override
    public void invoke(MiddlewareContext context, MiddlewareChain chain) throws Exception {
        String parameterName = context.getParameterName(ENTITY_TRIGGER);
        if (parameterName == null) {
            chain.doNext(context);
            return;
        }

        // The entity function receives the raw base64-encoded EntityBatchRequest as a String.
        // The user function is expected to call EntityRunner.loadAndRun() with a TaskEntityFactory
        // and return the base64-encoded EntityBatchResult.
        //
        // Unlike orchestrations, entity operations are simple request/response calls with no
        // replay-based blocking (no OrchestratorBlockedException equivalent), so the middleware
        // delegates directly to the user function.
        try {
            chain.doNext(context);
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof DataConverter.DataConverterException) {
                throw (DataConverter.DataConverterException) cause;
            }
            throw new RuntimeException("Unexpected failure in entity function execution", e);
        }
    }
}
