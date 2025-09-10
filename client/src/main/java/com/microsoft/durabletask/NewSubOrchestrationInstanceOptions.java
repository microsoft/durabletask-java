package com.microsoft.durabletask;

/**
 * Options for starting a new sub-orchestration instance.
 */
public class NewSubOrchestrationInstanceOptions extends TaskOptions {

    private String instanceId;
    private String version;

    /**
     * Creates default options for the sub-orchestration. Useful for chaining
     * when a RetryPolicy or RetryHandler is not needed.
     */
    public NewSubOrchestrationInstanceOptions() {
        super((RetryPolicy) null);
    }

    /**
     * Creates options with a retry policy for the sub-orchestration.
     * @param retryPolicy The retry policy to use for the sub-orchestration.
     */
    public NewSubOrchestrationInstanceOptions(RetryPolicy retryPolicy) {
        super(retryPolicy);
    }

    /**
     * Creates options with a retry handler for the sub-orchestration.
     * @param retryHandler The retry handler to use for the sub-orchestration.
     */
    public NewSubOrchestrationInstanceOptions(RetryHandler retryHandler) {
        super(retryHandler);
    }

    /**
     * Sets the version for the sub-orchestration instance.
     * @param version The version string to use.
     * @return This options object for chaining.
     */
    public NewSubOrchestrationInstanceOptions setVersion(String version) {
        this.version = version;
        return this;
    }

    /**
     * Gets the version for the sub-orchestration instance.
     * @return The version string, or null if not set.
     */
    public String getVersion() {
        return version;
    }

    /**
     * Sets a custom instance ID for the sub-orchestration.
     * @param instanceId The custom instance ID to use.
     * @return This options object for chaining.
     */
    public NewSubOrchestrationInstanceOptions setInstanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    /**
     * Gets the custom instance ID for the sub-orchestration.
     * @return The instance ID, or null if not set.
     */
    public String getInstanceId() {
        return instanceId;
    }
}
