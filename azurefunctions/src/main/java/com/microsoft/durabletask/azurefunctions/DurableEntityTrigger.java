/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.microsoft.durabletask.azurefunctions;

import com.microsoft.azure.functions.annotation.CustomBinding;
import com.microsoft.azure.functions.annotation.HasImplicitOutput;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Azure Functions attribute for binding a function parameter to a Durable Task entity request.
 * </p><p>
 * The following is an example of an entity function that uses this trigger binding to implement
 * a counter entity backed by a {@code TaskEntity} subclass.
 * </p>
 * <pre>
 * {@literal @}FunctionName("Counter")
 * public String counterEntity(
 *         {@literal @}DurableEntityTrigger(name = "req") String req) {
 *     return EntityRunner.loadAndRun(req, () -&gt; new CounterEntity());
 * }
 * </pre>
 *
 * @since 2.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
@CustomBinding(direction = "in", name = "", type = "entityTrigger")
@HasImplicitOutput
public @interface DurableEntityTrigger {
    /**
     * <p>The name of the entity function.</p>
     * <p>If not specified, the function name is used as the name of the entity.</p>
     * <p>This property supports binding parameters.</p>
     *
     * @return The name of the entity function.
     */
    String entityName() default "";

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
    String dataType() default "string";
}
