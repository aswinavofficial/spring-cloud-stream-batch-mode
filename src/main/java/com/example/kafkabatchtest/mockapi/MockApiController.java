package com.example.kafkabatchtest.mockapi;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock dependent API that processes batches of messages.
 * Simulates an external API that:
 * - Accepts batches of up to 5 messages
 * - Has a processing delay
 * - Returns success/failure responses
 */
@Slf4j
@RestController
@RequestMapping("/api/mock")
public class MockApiController {

    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong totalMessagesProcessed = new AtomicLong(0);
    private final AtomicLong successfulRequests = new AtomicLong(0);
    private final AtomicLong failedRequests = new AtomicLong(0);

    /**
     * Process a batch of messages.
     * Accepts up to 5 messages per request.
     */
    @PostMapping("/process")
    public ResponseEntity<ProcessResponse> processBatch(@RequestBody ProcessRequest request) {
        totalRequests.incrementAndGet();
        Instant startTime = Instant.now();

        log.info("Mock API: Received batch {} with {} messages from partition {}",
                request.getBatchId(),
                request.getMessages() != null ? request.getMessages().size() : 0,
                request.getPartition());

        // Validate request
        if (request.getMessages() == null || request.getMessages().isEmpty()) {
            failedRequests.incrementAndGet();
            return ResponseEntity.badRequest().body(ProcessResponse.builder()
                    .requestId(UUID.randomUUID().toString())
                    .batchId(request.getBatchId())
                    .success(false)
                    .message("No messages provided")
                    .processedCount(0)
                    .build());
        }

        if (request.getMessages().size() > 5) {
            failedRequests.incrementAndGet();
            return ResponseEntity.badRequest().body(ProcessResponse.builder()
                    .requestId(UUID.randomUUID().toString())
                    .batchId(request.getBatchId())
                    .success(false)
                    .message("Batch size exceeds maximum of 5 messages")
                    .processedCount(0)
                    .build());
        }

        // Simulate processing delay (10-50ms)
        try {
            Thread.sleep(10 + (long) (Math.random() * 40));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        int processedCount = request.getMessages().size();
        totalMessagesProcessed.addAndGet(processedCount);
        successfulRequests.incrementAndGet();

        long processingTimeMs = Instant.now().toEpochMilli() - startTime.toEpochMilli();

        log.debug("Mock API: Processed batch {} with {} messages in {}ms",
                request.getBatchId(), processedCount, processingTimeMs);

        return ResponseEntity.ok(ProcessResponse.builder()
                .requestId(UUID.randomUUID().toString())
                .batchId(request.getBatchId())
                .success(true)
                .message("Batch processed successfully")
                .processedCount(processedCount)
                .processingTimeMs(processingTimeMs)
                .build());
    }

    /**
     * Get mock API statistics.
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        return ResponseEntity.ok(Map.of(
                "totalRequests", totalRequests.get(),
                "successfulRequests", successfulRequests.get(),
                "failedRequests", failedRequests.get(),
                "totalMessagesProcessed", totalMessagesProcessed.get(),
                "averageMessagesPerRequest", totalRequests.get() > 0
                        ? (double) totalMessagesProcessed.get() / totalRequests.get()
                        : 0.0));
    }

    /**
     * Reset statistics.
     */
    @DeleteMapping("/stats")
    public ResponseEntity<Map<String, String>> resetStats() {
        totalRequests.set(0);
        totalMessagesProcessed.set(0);
        successfulRequests.set(0);
        failedRequests.set(0);
        return ResponseEntity.ok(Map.of("status", "success", "message", "Mock API stats reset"));
    }

    /**
     * Request body for process endpoint.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProcessRequest {
        private String batchId;
        private int partition;
        private List<String> messageIds;
        private List<String> messages;
    }

    /**
     * Response body for process endpoint.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProcessResponse {
        private String requestId;
        private String batchId;
        private boolean success;
        private String message;
        private int processedCount;
        private long processingTimeMs;
    }
}
