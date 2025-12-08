package com.example.kafkabatchtest.controller;

import com.example.kafkabatchtest.model.BatchAnalysisResult;
import com.example.kafkabatchtest.producer.TestMessageProducer;
import com.example.kafkabatchtest.service.BatchAnalysisService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST API for testing Kafka batch consumption.
 * Provides endpoints to:
 * - Send test messages to specific partitions
 * - View batch consumption analysis results
 * - Get statistics on partition distribution
 */
@Slf4j
@RestController
@RequestMapping("/api/test")
@RequiredArgsConstructor
public class TestController {

    private final TestMessageProducer producer;
    private final BatchAnalysisService analysisService;

    /**
     * Send messages to a specific partition.
     * 
     * @param partition Target partition (0, 1, or 2)
     * @param count     Number of messages to send (default: 5)
     */
    @PostMapping("/send/{partition}")
    public ResponseEntity<Map<String, Object>> sendToPartition(
            @PathVariable int partition,
            @RequestParam(defaultValue = "5") int count) {

        log.info("API: Sending {} messages to partition {}", count, partition);

        try {
            producer.sendToPartition(partition, count);

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("partition", partition);
            response.put("messageCount", count);
            response.put("message", String.format("Sent %d messages to partition %d", count, partition));

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("status", "error");
            error.put("message", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * Send messages evenly to all partitions.
     * 
     * @param messagesPerPartition Number of messages per partition (default: 10)
     */
    @PostMapping("/send/all")
    public ResponseEntity<Map<String, Object>> sendToAllPartitions(
            @RequestParam(defaultValue = "10") int messagesPerPartition) {

        log.info("API: Sending {} messages to each partition", messagesPerPartition);

        int totalSent = producer.sendToAllPartitions(messagesPerPartition);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("messagesPerPartition", messagesPerPartition);
        response.put("totalMessagesSent", totalSent);
        response.put("partitions", 3);

        return ResponseEntity.ok(response);
    }

    /**
     * Run a stress test with many messages.
     * 
     * @param totalMessages Total messages to send (default: 60)
     */
    @PostMapping("/stress")
    public ResponseEntity<Map<String, Object>> stressTest(
            @RequestParam(defaultValue = "60") int totalMessages) {

        log.info("API: Starting stress test with {} messages", totalMessages);

        int totalSent = producer.sendStressTest(totalMessages);

        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("requestedMessages", totalMessages);
        response.put("actualMessagesSent", totalSent);

        return ResponseEntity.ok(response);
    }

    /**
     * Get all batch analysis results.
     */
    @GetMapping("/results")
    public ResponseEntity<List<BatchAnalysisResult>> getResults() {
        return ResponseEntity.ok(analysisService.getAllResults());
    }

    /**
     * Get statistics on batch consumption.
     */
    @GetMapping("/statistics")
    public ResponseEntity<BatchAnalysisService.BatchStatistics> getStatistics() {
        return ResponseEntity.ok(analysisService.getStatistics());
    }

    /**
     * Get a detailed report on batch partition distribution.
     */
    @GetMapping("/report")
    public ResponseEntity<Map<String, Object>> getReport() {
        var stats = analysisService.getStatistics();
        var results = analysisService.getAllResults();

        Map<String, Object> report = new HashMap<>();
        report.put("configuration", Map.of(
                "batchMode", true,
                "concurrency", 3,
                "maxPollRecords", 5,
                "partitions", 3));
        report.put("statistics", stats);
        report.put("summary", stats.getSummary());

        // Analyze whether the expectation is met
        boolean expectationMet = stats.getSamePartitionBatches() == stats.getTotalBatches();
        report.put("expectationMet", expectationMet);
        report.put("analysis", Map.of(
                "question", "Do all batches contain messages from the same partition?",
                "answer", expectationMet ? "YES - All batches have messages from single partition"
                        : "NO - Some batches have messages from multiple partitions",
                "batchesWithMixedPartitions", stats.getMixedPartitionBatches(),
                "percentageValid", stats.getTotalBatches() > 0
                        ? (stats.getValidBatches() * 100.0 / stats.getTotalBatches())
                        : 0));

        // Recent batches sample
        int sampleSize = Math.min(10, results.size());
        report.put("recentBatches", results.subList(
                Math.max(0, results.size() - sampleSize),
                results.size()));

        return ResponseEntity.ok(report);
    }

    /**
     * Clear all recorded results.
     */
    @DeleteMapping("/clear")
    public ResponseEntity<Map<String, String>> clearResults() {
        analysisService.clearResults();
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "All batch analysis results cleared"));
    }

    /**
     * Health check and configuration info.
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> info() {
        Map<String, Object> info = new HashMap<>();
        info.put("application", "Kafka Batch Test");
        info.put("configuration", Map.of(
                "batchMode", true,
                "concurrency", 3,
                "maxPollRecords", 5,
                "partitions", 3,
                "topic", "test-batch-topic"));
        info.put("endpoints", Map.of(
                "POST /api/test/send/{partition}?count=N", "Send N messages to specific partition",
                "POST /api/test/send/all?messagesPerPartition=N", "Send N messages to each partition",
                "POST /api/test/stress?totalMessages=N", "Run stress test with N total messages",
                "GET /api/test/results", "Get all batch analysis results",
                "GET /api/test/statistics", "Get consumption statistics",
                "GET /api/test/report", "Get detailed analysis report",
                "DELETE /api/test/clear", "Clear all results"));
        return ResponseEntity.ok(info);
    }
}
