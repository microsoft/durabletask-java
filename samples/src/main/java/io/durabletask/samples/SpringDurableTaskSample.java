// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package io.durabletask.samples;

import com.azure.core.credential.AccessToken;
import com.microsoft.durabletask.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.annotation.Value;
import java.time.Duration;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.CallOptions;
import io.grpc.ChannelCredentials;
import io.grpc.TlsChannelCredentials;
import io.grpc.InsecureChannelCredentials;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import java.util.Objects;
import com.azure.identity.DefaultAzureCredentialBuilder;
import reactor.core.publisher.Mono;

@ConfigurationProperties(prefix = "durable.task")
@lombok.Data
class DurableTaskProperties {
    private String endpoint;
    private String hubName;
    private String resourceId = "https://durabletask.io";
    private boolean allowInsecure = false;
}

/**
 * Sample Spring Boot application demonstrating Azure Durable Task integration.
 * This sample shows how to:
 * 1. Configure Durable Task with Spring Boot
 * 2. Create orchestrations and activities
 * 3. Handle REST API endpoints for order processing
 */
@SpringBootApplication
@EnableConfigurationProperties(DurableTaskProperties.class)
public class SpringDurableTaskSample {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(SpringDurableTaskSample.class, args);
    
        // Get the worker bean and start it
        DurableTaskGrpcWorker worker = context.getBean(DurableTaskGrpcWorker.class);
        worker.start();
    }

    @Configuration
    static class DurableTaskConfig {

        @Bean
        public TokenCredential tokenCredential() {
            return new DefaultAzureCredentialBuilder().build();
        }

        @Bean
        public AccessTokenCache accessTokenCache(
                TokenCredential credential,
                DurableTaskProperties properties) {
            if (credential == null) {
                return null;
            }
            TokenRequestContext context = new TokenRequestContext();
            context.addScopes(new String[] { properties.getResourceId() + "/.default" });
            return new AccessTokenCache(
                credential, context, Duration.ofMinutes(5)
            );
        }

        @Bean(name = "workerChannel")
        public Channel workerGrpcChannel(
                DurableTaskProperties properties,
                AccessTokenCache tokenCache) {
            return createChannel(properties, tokenCache);
        }

        @Bean(name = "clientChannel")
        public Channel clientGrpcChannel(
                DurableTaskProperties properties,
                AccessTokenCache tokenCache) {
            return createChannel(properties, tokenCache);
        }

        private Channel createChannel(DurableTaskProperties properties, AccessTokenCache tokenCache) {
            Objects.requireNonNull(properties.getHubName(), "taskHubName must not be null");
            
            // Normalize the endpoint URL and add DNS scheme for gRPC name resolution
            String endpoint = "dns:///" + properties.getEndpoint();

            // Create metadata interceptor to add task hub name and auth token
            ClientInterceptor metadataInterceptor = new ClientInterceptor() {
                @Override
                public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                        MethodDescriptor<ReqT, RespT> method,
                        CallOptions callOptions,
                        Channel next) {
                    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                            next.newCall(method, callOptions)) {
                        @Override
                        public void start(ClientCall.Listener<RespT> responseListener, Metadata headers) {
                            headers.put(
                                Metadata.Key.of("taskhub", Metadata.ASCII_STRING_MARSHALLER),
                                properties.getHubName()
                            );
                            
                            // Add authorization token if credentials are configured
                            if (tokenCache != null) {
                                String token = tokenCache.getToken().getToken();
                                headers.put(
                                    Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER),
                                    "Bearer " + token
                                );
                            }
                            
                            super.start(responseListener, headers);
                        }
                    };
                }
            };
            
            // Build the channel with appropriate security settings
            ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(endpoint)
                .intercept(metadataInterceptor);
                
            if (!properties.isAllowInsecure()) {
                builder.useTransportSecurity();
            } else {
                builder.usePlaintext();
            }

            return builder.build();
        }

        @Bean(destroyMethod = "stop")
        public DurableTaskGrpcWorker durableTaskWorker(@Qualifier("workerChannel") Channel grpcChannel) {
            DurableTaskGrpcWorkerBuilder builder = new DurableTaskGrpcWorkerBuilder()
                .grpcChannel(grpcChannel);
            
            // Add orchestrations
            builder.addOrchestration(new TaskOrchestrationFactory() {
                @Override
                public String getName() { 
                    return "ProcessOrderOrchestration"; 
                }

                @Override
                public TaskOrchestration create() {
                    return ctx -> {
                        // Get the order input as JSON string
                        String orderJson = ctx.getInput(String.class);

                        // Process the order through multiple activities
                        boolean isValid = ctx.callActivity("ValidateOrder", orderJson, Boolean.class).await();
                        if (!isValid) {
                            ctx.complete("{\"status\": \"FAILED\", \"message\": \"Order validation failed\"}");
                            return;
                        }

                        // Process payment
                        String paymentResult = ctx.callActivity("ProcessPayment", orderJson, String.class).await();
                        if (!paymentResult.contains("\"success\":true")) {
                            ctx.complete("{\"status\": \"FAILED\", \"message\": \"Payment processing failed\"}");
                            return;
                        }

                        // Ship order
                        String shipmentResult = ctx.callActivity("ShipOrder", orderJson, String.class).await();
                        
                        // Return the final result
                        ctx.complete("{\"status\": \"SUCCESS\", " +
                                   "\"payment\": " + paymentResult + ", " +
                                   "\"shipment\": " + shipmentResult + "}");
                    };
                }
            });

            // Add activity implementations
            builder.addActivity(new TaskActivityFactory() {
                @Override
                public String getName() { return "ValidateOrder"; }

                @Override
                public TaskActivity create() {
                    return ctx -> {
                        String orderJson = ctx.getInput(String.class);
                        // Simple validation - check if order contains amount and it's greater than 0
                        return orderJson.contains("\"amount\"") && !orderJson.contains("\"amount\":0");
                    };
                }
            });

            builder.addActivity(new TaskActivityFactory() {
                @Override
                public String getName() { return "ProcessPayment"; }

                @Override
                public TaskActivity create() {
                    return ctx -> {
                        String orderJson = ctx.getInput(String.class);
                        // Simulate payment processing
                        sleep(1000); // Simulate processing time
                        return "{\"success\":true, \"transactionId\":\"TXN" + System.currentTimeMillis() + "\"}";
                    };
                }
            });

            builder.addActivity(new TaskActivityFactory() {
                @Override
                public String getName() { return "ShipOrder"; }

                @Override
                public TaskActivity create() {
                    return ctx -> {
                        String orderJson = ctx.getInput(String.class);
                        // Simulate shipping process
                        sleep(1000); // Simulate processing time
                        return "{\"trackingNumber\":\"TRACK" + System.currentTimeMillis() + "\"}";
                    };
                }
            });

            return builder.build();
        }

        @Bean
        public DurableTaskClient durableTaskClient(@Qualifier("clientChannel") Channel grpcChannel) {
            return new DurableTaskGrpcClientBuilder()
                .grpcChannel(grpcChannel)
                .build();
        }
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}

/**
 * REST Controller for handling order-related operations.
 */
@RestController
@RequestMapping("/api/orders")
class OrderController {
    
    private final DurableTaskClient client;

    public OrderController(DurableTaskClient client) {
        this.client = client;
    }

    @PostMapping
    public String createOrder(@RequestBody String orderJson) throws Exception {
        String instanceId = client.scheduleNewOrchestrationInstance(
            "ProcessOrderOrchestration", 
            orderJson
        );
        
        // Wait for the orchestration to complete with a timeout
        OrchestrationMetadata metadata = client.waitForInstanceCompletion(
            instanceId, 
            Duration.ofSeconds(30), 
            true
        );

        if (metadata.getRuntimeStatus() == OrchestrationRuntimeStatus.COMPLETED) {
            return metadata.readOutputAs(String.class);
        } else {
            return "{\"status\": \"" + metadata.getRuntimeStatus() + "\"}";
        }
    }

    @GetMapping("/{instanceId}")
    public String getOrder(@PathVariable String instanceId) throws Exception {
        OrchestrationMetadata metadata = client.getInstanceMetadata(instanceId, true);
        if (metadata == null) {
            return "{\"error\": \"Order not found\"}";
        }

        if (metadata.getRuntimeStatus() == OrchestrationRuntimeStatus.COMPLETED) {
            return metadata.readOutputAs(String.class);
        } else {
            return "{\"status\": \"" + metadata.getRuntimeStatus() + "\"}";
        }
    }
} 
