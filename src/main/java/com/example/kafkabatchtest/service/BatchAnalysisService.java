package com.example.kafkabatchtest.service;

import com.example.kafkabatchtest.model.BatchAnalysisResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Service to track and analyze batch consumption results.
 * This helps verify whether each batch contains messages from the same
 * partition.
 * 
 * Thread Safety (Virtual Thread Compatible - Java 21+):
 * - Uses CopyOnWriteArrayList instead of synchronized list to prevent carrier
 * thread pinning
 */
@Slf4j
@Service
public class BatchAnalysisService {

        // CopyOnWriteArrayList is virtual thread friendly - no synchronized blocks
        private final List<BatchAnalysisResult> results = new CopyOnWriteArrayList<>();
        private final AtomicInteger batchCounter = new AtomicInteger(0);
        private final ConcurrentHashMap<String, AtomicInteger> threadBatchCounts = new ConcurrentHashMap<>();

        /**
         * Record a new batch analysis result.
         */
        public void recordBatch(BatchAnalysisResult result) {
                results.add(result);

                // Track per-thread batch counts
                threadBatchCounts.computeIfAbsent(result.getConsumerThread(), k -> new AtomicInteger(0))
                                .incrementAndGet();

                int batchNum = batchCounter.incrementAndGet();

                // Log the result with clear formatting
                log.info("\n" +
                                "╔══════════════════════════════════════════════════════════════════╗\n" +
                                "║                    BATCH #{} RECEIVED                            ║\n" +
                                "╠══════════════════════════════════════════════════════════════════╣\n" +
                                "║  Thread:          {}                                             \n" +
                                "║  Message Count:   {}                                             \n" +
                                "║  Partitions:      {}                                             \n" +
                                "║  Same Partition:  {}                                             \n" +
                                "║  Valid (5 msgs, same partition): {}                              \n" +
                                "╚══════════════════════════════════════════════════════════════════╝",
                                batchNum,
                                result.getConsumerThread(),
                                result.getMessageCount(),
                                result.getPartitionsInBatch(),
                                result.isAllMessagesFromSamePartition() ? "✓ YES" : "✗ NO",
                                result.isValid() ? "✓ VALID" : "✗ INVALID");
        }

        /**
         * Get all recorded batch results.
         */
        public List<BatchAnalysisResult> getAllResults() {
                return new ArrayList<>(results);
        }

        /**
         * Get aggregated statistics.
         */
        public BatchStatistics getStatistics() {
                List<BatchAnalysisResult> allResults = new ArrayList<>(results);

                int totalBatches = allResults.size();
                long validBatches = allResults.stream().filter(BatchAnalysisResult::isValid).count();
                long samePartitionBatches = allResults.stream()
                                .filter(BatchAnalysisResult::isAllMessagesFromSamePartition).count();
                long batchesWithFiveMessages = allResults.stream()
                                .filter(r -> r.getMessageCount() == 5).count();

                // Count batches with mixed partitions (not all from same partition)
                long mixedPartitionBatches = allResults.stream()
                                .filter(r -> !r.isAllMessagesFromSamePartition()).count();

                // Distribution of batch sizes
                var batchSizeDistribution = allResults.stream()
                                .collect(Collectors.groupingBy(BatchAnalysisResult::getMessageCount,
                                                Collectors.counting()));

                // Thread distribution
                var threadDistribution = new ConcurrentHashMap<>(threadBatchCounts);

                return BatchStatistics.builder()
                                .totalBatches(totalBatches)
                                .validBatches(validBatches)
                                .samePartitionBatches(samePartitionBatches)
                                .batchesWithFiveMessages(batchesWithFiveMessages)
                                .mixedPartitionBatches(mixedPartitionBatches)
                                .batchSizeDistribution(batchSizeDistribution)
                                .threadDistribution(threadDistribution.entrySet().stream()
                                                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().get())))
                                .build();
        }

        /**
         * Clear all recorded results.
         */
        public void clearResults() {
                results.clear();
                batchCounter.set(0);
                threadBatchCounts.clear();
                log.info("Cleared all batch analysis results");
        }

        @lombok.Data
        @lombok.Builder
        @lombok.NoArgsConstructor
        @lombok.AllArgsConstructor
        public static class BatchStatistics {
                private int totalBatches;
                private long validBatches;
                private long samePartitionBatches;
                private long batchesWithFiveMessages;
                private long mixedPartitionBatches;
                private java.util.Map<Integer, Long> batchSizeDistribution;
                private java.util.Map<String, Integer> threadDistribution;

                public String getSummary() {
                        return String.format(
                                        "Total Batches: %d | Valid (5 msgs, same partition): %d (%.1f%%) | " +
                                                        "Same Partition: %d | Mixed Partitions: %d",
                                        totalBatches,
                                        validBatches,
                                        totalBatches > 0 ? (validBatches * 100.0 / totalBatches) : 0,
                                        samePartitionBatches,
                                        mixedPartitionBatches);
                }
        }
}
