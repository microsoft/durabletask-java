package com.microsoft.durabletask;

import com.microsoft.durabletask.implementation.protobuf.OrchestratorService;
public enum OrchestrationQueryOrder {
    /**
     * Unspecified sort order of query result, this is default value.
     */
    UNSPECIFIED,
    /**
     * Ascending sort order of query result, this is default value.
     */
    ASCENDING,
    /**
     * Descending sort order of query result, this is default value.
     */
    DESCENDING;

    static OrchestrationQueryOrder fromProtobuf(OrchestratorService.Order order) {
        switch (order) {
            case Unspecified:
                return UNSPECIFIED;
            case Ascending:
                return ASCENDING;
            case Descending:
                return DESCENDING;
            default:
                throw new IllegalArgumentException(String.format("Unknown sort order: %s", order));
        }
    }

    static OrchestratorService.Order toProtobuf(OrchestrationQueryOrder order){
        switch (order) {
            case UNSPECIFIED:
                return OrchestratorService.Order.Unspecified;
            case ASCENDING:
                return OrchestratorService.Order.Ascending;
            case DESCENDING:
                return OrchestratorService.Order.Descending;
            default:
                throw new IllegalArgumentException(String.format("unknown sort order: %s", order));
        }
    }
}
