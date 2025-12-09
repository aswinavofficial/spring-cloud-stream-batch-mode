package com.example.kafkabatchtest.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * Batch of messages to send to the dependent API.
 * Each batch contains up to 5 messages from the same partition.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ApiRequestBatch {

    private String batchId;
    private int partition;
    private Instant createdAt;
    private List<String> messageIds;
    private List<String> messagePayloads;
    private int messageCount;

    /**
     * Create a batch from a list of message payloads.
     */
    public static ApiRequestBatch create(String batchId, int partition, List<String> messageIds,
            List<String> payloads) {
        return ApiRequestBatch.builder()
                .batchId(batchId)
                .partition(partition)
                .createdAt(Instant.now())
                .messageIds(messageIds)
                .messagePayloads(payloads)
                .messageCount(payloads.size())
                .build();
    }
}
