package com.example.kafkabatchtest.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Result of an API call to the dependent service.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ApiCallResult {

    private String batchId;
    private int partition;
    private boolean success;
    private int httpStatus;
    private String responseBody;
    private String errorMessage;
    private Instant calledAt;
    private Instant completedAt;
    private long durationMs;

    // Rate limiting info
    private boolean wasDelayed;
    private long waitTimeMs;

    // Batch info
    private int messageCount;

    /**
     * Create a successful result.
     */
    public static ApiCallResult success(String batchId, int partition, int messageCount,
            int httpStatus, String responseBody,
            long durationMs, boolean wasDelayed, long waitTimeMs) {
        Instant now = Instant.now();
        return ApiCallResult.builder()
                .batchId(batchId)
                .partition(partition)
                .success(true)
                .httpStatus(httpStatus)
                .responseBody(responseBody)
                .calledAt(now.minusMillis(durationMs))
                .completedAt(now)
                .durationMs(durationMs)
                .wasDelayed(wasDelayed)
                .waitTimeMs(waitTimeMs)
                .messageCount(messageCount)
                .build();
    }

    /**
     * Create a failed result.
     */
    public static ApiCallResult failure(String batchId, int partition, int messageCount,
            String errorMessage, long durationMs,
            boolean wasDelayed, long waitTimeMs) {
        Instant now = Instant.now();
        return ApiCallResult.builder()
                .batchId(batchId)
                .partition(partition)
                .success(false)
                .httpStatus(0)
                .errorMessage(errorMessage)
                .calledAt(now.minusMillis(durationMs))
                .completedAt(now)
                .durationMs(durationMs)
                .wasDelayed(wasDelayed)
                .waitTimeMs(waitTimeMs)
                .messageCount(messageCount)
                .build();
    }
}
