/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.durabletask.azurefunctions;

import com.microsoft.azure.functions.annotation.CustomBinding;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Azure Functions attribute for binding a function parameter to a {@literal @}DurableClientContext object.
 * </p><p>
 * The following is an example of an HTTP-trigger function that uses this input binding to start a new
 * orchestration instance.
 * </p>
 * <pre>
 * {@literal @}FunctionName("StartHelloCities")
 * public HttpResponseMessage startHelloCities(
 *         {@literal @}HttpTrigger(name = "request", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage{@literal <}Optional{@literal <}String{@literal >}{@literal >} request,
 *         {@literal @}DurableClientInput(name = "durableContext") DurableClientContext durableContext,
 *         final ExecutionContext context) {
 * 
 *     DurableTaskClient client = durableContext.getClient();
 *     String instanceId = client.scheduleNewOrchestrationInstance("HelloCities");
 *     context.getLogger().info("Created new Java orchestration with instance ID = " + instanceId);
 *     return durableContext.createCheckStatusResponse(request, instanceId);
 * }
 * </pre>
 * 
 * @since 2.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
@CustomBinding(direction = "in", name = "", type = "durableClient")
public @interface DurableClientInput {
    /**
     * The variable name used in function.json.
     * 
     * @return The variable name used in function.json.
     */
    String name();

    /**
     * <p>
     * Defines how Functions runtime should treat the parameter value. Possible values are:
     * </p>
     * <ul>
     * <li>"": get the value as a string, and try to deserialize to actual parameter type like POJO</li>
     * <li>string: always get the value as a string</li>
     * <li>binary: get the value as a binary data, and try to deserialize to actual parameter type byte[]</li>
     * </ul>
     * 
     * @return The dataType which will be used by the Functions runtime.
     */
    String dataType() default "";

    /**
     * <p>
     * Optional. The name of the task hub in which the orchestration data lives.
     * </p>
     * <p>
     * If not specified, the task hub name used by this binding will be the value specified in host.json.
     * If a task hub name is not configured in host.json and if the function app is running in the 
     * Azure Functions hosted service, then task hub name is derived from the function app's name.
     * Otherwise, a constant value is used for the task hub name.
     * </p>
     * <p>
     * In general, you should <i>not</i> set a value for the task hub name here unless you intend to configure
     * the client to interact with orchestrations in another app.
     * </p>
     * 
     * @return The task hub name to use for the client.
     */
    String taskHub() default "";
}
