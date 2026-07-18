// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.durabletask.exporthistory;

/**
 * Valid state-transition rules for export jobs. The operation-name constants match the {@link ExportJob} entity
 * method names (case-insensitive dispatch).
 */
final class ExportJobTransitions {

    /** The {@code Create} operation name. */
    public static final String OP_CREATE = "Create";

    /** The {@code Get} operation name. */
    public static final String OP_GET = "Get";

    /** The {@code Run} operation name. */
    public static final String OP_RUN = "Run";

    /** The {@code CommitCheckpoint} operation name. */
    public static final String OP_COMMIT_CHECKPOINT = "CommitCheckpoint";

    /** The {@code MarkAsCompleted} operation name. */
    public static final String OP_MARK_AS_COMPLETED = "MarkAsCompleted";

    /** The {@code MarkAsFailed} operation name. */
    public static final String OP_MARK_AS_FAILED = "MarkAsFailed";

    /** The {@code Delete} operation name. */
    public static final String OP_DELETE = "Delete";

    private ExportJobTransitions() {
    }

    /**
     * Checks whether a transition to the target state is valid for the given operation and current state.
     *
     * @param operationName the name of the operation being performed
     * @param from          the current export job status
     * @param targetState   the target status to transition to
     * @return {@code true} if the transition is valid; otherwise {@code false}
     */
    public static boolean isValidTransition(String operationName, ExportJobStatus from, ExportJobStatus targetState) {
        if (operationName == null) {
            return false;
        }
        switch (operationName) {
            case OP_CREATE:
                return targetState == ExportJobStatus.ACTIVE
                        && (from == ExportJobStatus.PENDING
                            || from == ExportJobStatus.FAILED
                            || from == ExportJobStatus.COMPLETED);
            case OP_MARK_AS_COMPLETED:
                return from == ExportJobStatus.ACTIVE && targetState == ExportJobStatus.COMPLETED;
            case OP_MARK_AS_FAILED:
                return from == ExportJobStatus.ACTIVE && targetState == ExportJobStatus.FAILED;
            default:
                return false;
        }
    }
}
