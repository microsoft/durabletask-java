package com.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.durabletask.DurableTaskClient;
import com.microsoft.durabletask.TaskOrchestrationContext;
import com.microsoft.durabletask.azurefunctions.DurableActivityTrigger;
import com.microsoft.durabletask.azurefunctions.DurableClientContext;
import com.microsoft.durabletask.azurefunctions.DurableClientInput;
import com.microsoft.durabletask.azurefunctions.DurableOrchestrationTrigger;

import java.time.LocalDate;
import java.util.Optional;

public class ExampleFunction {

    @FunctionName("StartExampleProcess")
    public HttpResponseMessage startExampleProcess(
            @HttpTrigger(name = "req",
                    methods = {HttpMethod.GET, HttpMethod.POST},
                    authLevel = AuthorizationLevel.ANONYMOUS) final HttpRequestMessage<Optional<String>> request,
            @DurableClientInput(name = "durableContext") final DurableClientContext durableContext,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request");

        final DurableTaskClient client = durableContext.getClient();
        final String instanceId = client.scheduleNewOrchestrationInstance("ExampleProcess");
        return durableContext.createCheckStatusResponse(request, instanceId);
    }

    @FunctionName("ExampleProcess")
    public ExampleResponse exampleOrchestrator(
            @DurableOrchestrationTrigger(name = "taskOrchestrationContext") final TaskOrchestrationContext context,
            final ExecutionContext functionContext) {
        return context.callActivity("ToLower", "Foo", ExampleResponse.class).await();
    }

    @FunctionName("ToLower")
    public ExampleResponse toLower(
            @DurableActivityTrigger(name = "value") final String value,
            final ExecutionContext context) {
        return new ExampleResponse(LocalDate.now(), value.toLowerCase());
    }

    static class ExampleResponse {
        private LocalDate date;
        private String value;

        public ExampleResponse(LocalDate date, String value) {
            this.date = date;
            this.value = value;
        }
    }
}
