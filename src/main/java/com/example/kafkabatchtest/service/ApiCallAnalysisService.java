package com.example.kafkabatchtest.service;

import com.example.kafkabatchtest.model.ApiCallResult;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Service to track and analyze API calls to the dependent service.
 */
@Slf4j
@Service
public class ApiCallAnalysisService {

    private final List<ApiCallResult> results = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong totalCalls = new AtomicLong(0);
    private final AtomicLong successfulCalls = new AtomicLong(0);
    private final AtomicLong failedCalls = new AtomicLong(0);
    private final AtomicLong delayedCalls = new AtomicLong(0);
    private final AtomicLong totalMessagesProcessed = new AtomicLong(0);
    private final AtomicLong totalWaitTimeMs = new AtomicLong(0);
    private final ConcurrentHashMap<Integer, AtomicLong> callsPerPartition = new ConcurrentHashMap<>();

    /**
     * Record an API call result.
     */
    public void recordApiCall(ApiCallResult result) {
        results.add(result);
        totalCalls.incrementAndGet();

        if (result.isSuccess()) {
            successfulCalls.incrementAndGet();
            totalMessagesProcessed.addAndGet(result.getMessageCount());
        } else {
            failedCalls.incrementAndGet();
        }

        if (result.isWasDelayed()) {
            delayedCalls.incrementAndGet();
            totalWaitTimeMs.addAndGet(result.getWaitTimeMs());
        }

        callsPerPartition.computeIfAbsent(result.getPartition(), k -> new AtomicLong(0))
                .incrementAndGet();

        log.info("\n" +
                "╔══════════════════════════════════════════════════════════════════╗\n" +
                "║                    API CALL #{} COMPLETED                        ║\n" +
                "╠══════════════════════════════════════════════════════════════════╣\n" +
                "║  Batch ID:        {}                                             \n" +
                "║  Partition:       {}                                             \n" +
                "║  Messages:        {}                                             \n" +
                "║  Success:         {}                                             \n" +
                "║  Duration:        {}ms                                           \n" +
                "║  Rate Limited:    {} (wait: {}ms)                                \n" +
                "╚══════════════════════════════════════════════════════════════════╝",
                totalCalls.get(),
                result.getBatchId(),
                result.getPartition(),
                result.getMessageCount(),
                result.isSuccess() ? "✓ YES" : "✗ NO",
                result.getDurationMs(),
                result.isWasDelayed() ? "✓ YES" : "✗ NO",
                result.getWaitTimeMs());
    }

    /**
     * Get all recorded results.
     */
    public List<ApiCallResult> getAllResults() {
        return new ArrayList<>(results);
    }

    /**
     * Get statistics.
     */
    public ApiCallStatistics getStatistics() {
        List<ApiCallResult> allResults = new ArrayList<>(results);

        // Calculate average duration
        double avgDuration = allResults.stream()
                .mapToLong(ApiCallResult::getDurationMs)
                .average()
                .orElse(0.0);

        // Per-partition breakdown
        Map<Integer, PartitionApiStats> partitionStats = new HashMap<>();
        for (Map.Entry<Integer, AtomicLong> entry : callsPerPartition.entrySet()) {
            int partition = entry.getKey();
            long calls = entry.getValue().get();

            List<ApiCallResult> partitionResults = allResults.stream()
                    .filter(r -> r.getPartition() == partition)
                    .collect(Collectors.toList());

            long partitionSuccess = partitionResults.stream().filter(ApiCallResult::isSuccess).count();
            long partitionDelayed = partitionResults.stream().filter(ApiCallResult::isWasDelayed).count();
            long partitionMessages = partitionResults.stream().mapToInt(ApiCallResult::getMessageCount).sum();
            long partitionWaitTime = partitionResults.stream().mapToLong(ApiCallResult::getWaitTimeMs).sum();

            partitionStats.put(partition, PartitionApiStats.builder()
                    .partition(partition)
                    .totalCalls(calls)
                    .successfulCalls(partitionSuccess)
                    .failedCalls(calls - partitionSuccess)
                    .delayedCalls(partitionDelayed)
                    .messagesProcessed(partitionMessages)
                    .totalWaitTimeMs(partitionWaitTime)
                    .averageWaitTimeMs(partitionDelayed > 0 ? (double) partitionWaitTime / partitionDelayed : 0.0)
                    .build());
        }

        return ApiCallStatistics.builder()
                .totalCalls(totalCalls.get())
                .successfulCalls(successfulCalls.get())
                .failedCalls(failedCalls.get())
                .delayedCalls(delayedCalls.get())
                .totalMessagesProcessed(totalMessagesProcessed.get())
                .totalWaitTimeMs(totalWaitTimeMs.get())
                .averageDurationMs(avgDuration)
                .averageWaitTimeMs(delayedCalls.get() > 0 ? (double) totalWaitTimeMs.get() / delayedCalls.get() : 0.0)
                .delayedPercentage(totalCalls.get() > 0 ? (delayedCalls.get() * 100.0 / totalCalls.get()) : 0.0)
                .successRate(totalCalls.get() > 0 ? (successfulCalls.get() * 100.0 / totalCalls.get()) : 0.0)
                .partitionStats(partitionStats)
                .build();
    }

    /**
     * Clear all results.
     */
    public void clearResults() {
        results.clear();
        totalCalls.set(0);
        successfulCalls.set(0);
        failedCalls.set(0);
        delayedCalls.set(0);
        totalMessagesProcessed.set(0);
        totalWaitTimeMs.set(0);
        callsPerPartition.clear();
        log.info("Cleared all API call analysis results");
    }

    /**
     * Statistics for API calls.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ApiCallStatistics {
        private long totalCalls;
        private long successfulCalls;
        private long failedCalls;
        private long delayedCalls;
        private long totalMessagesProcessed;
        private long totalWaitTimeMs;
        private double averageDurationMs;
        private double averageWaitTimeMs;
        private double delayedPercentage;
        private double successRate;
        private Map<Integer, PartitionApiStats> partitionStats;

        public String getSummary() {
            return String.format(
                    "API Calls: %d (Success: %.1f%%) | Delayed: %d (%.1f%%) | Messages: %d | Avg Wait: %.1fms",
                    totalCalls, successRate, delayedCalls, delayedPercentage,
                    totalMessagesProcessed, averageWaitTimeMs);
        }
    }

    /**
     * Per-partition API call statistics.
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PartitionApiStats {
        private int partition;
        private long totalCalls;
        private long successfulCalls;
        private long failedCalls;
        private long delayedCalls;
        private long messagesProcessed;
        private long totalWaitTimeMs;
        private double averageWaitTimeMs;
    }
}
