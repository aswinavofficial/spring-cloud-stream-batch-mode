package com.example.kafkabatchtest.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * Analysis result for each batch received by the consumer.
 * This helps verify whether messages in a batch come from the same partition.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BatchAnalysisResult {

    private String batchId;
    private Instant receivedAt;
    private String consumerThread;
    private int messageCount;
    private Set<Integer> partitionsInBatch;
    private boolean allMessagesFromSamePartition;
    private List<String> messageIds;
    private String summary;

    /**
     * Returns true if this batch satisfies the expected behavior:
     * - Contains exactly 5 messages (max.poll.records)
     * - All messages are from the same partition
     */
    public boolean isValid() {
        return messageCount == 5 && allMessagesFromSamePartition;
    }
}
