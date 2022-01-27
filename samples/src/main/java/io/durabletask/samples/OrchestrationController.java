// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.microsoft.durabletask.*;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrchestrationController {

    final DurableTaskClient client;

    public OrchestrationController() {
        this.client = DurableTaskGrpcClient.newBuilder().build();
    }

    @GetMapping("/hello")
    public String greeting(@RequestParam(value = "name", defaultValue = "World") String name) {
        return String.format("Hello, %s!", name);
    }

    @GetMapping("/placeOrder")
    public NewOrderResponse placeOrder(@RequestParam(value = "item") String item) {
        String instanceId = this.client.scheduleNewOrchestrationInstance(
            "ProcessOrderOrchestration",
            NewOrchestrationInstanceOptions.newBuilder().setInput(item).build());
        return new NewOrderResponse(instanceId);
    }

    private class NewOrderResponse {
        private final String instanceId;

        public NewOrderResponse(String instanceId) {
            this.instanceId = instanceId;
        }

        public String getInstanceId() {
            return this.instanceId;
        }
    }
}