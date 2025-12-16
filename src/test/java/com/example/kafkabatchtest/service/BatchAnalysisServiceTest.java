package com.example.kafkabatchtest.service;

import com.example.kafkabatchtest.model.BatchAnalysisResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for BatchAnalysisService.
 * 
 * Verifies thread safety using CopyOnWriteArrayList (virtual thread
 * compatible).
 */
class BatchAnalysisServiceTest {

    private BatchAnalysisService service;

    @BeforeEach
    void setUp() {
        service = new BatchAnalysisService();
    }

    private BatchAnalysisResult createBatch(String batchId, int partition, int messageCount) {
        return BatchAnalysisResult.builder()
                .batchId(batchId)
                .receivedAt(Instant.now())
                .consumerThread("thread-" + partition)
                .messageCount(messageCount)
                .partitionsInBatch(Set.of(partition))
                .allMessagesFromSamePartition(true)
                .messageIds(List.of("msg1", "msg2"))
                .summary("Test batch")
                .build();
    }

    @Test
    @DisplayName("Should record batch result")
    void shouldRecordBatchResult() {
        BatchAnalysisResult batch = createBatch("batch-1", 0, 5);

        service.recordBatch(batch);

        List<BatchAnalysisResult> results = service.getAllResults();
        assertEquals(1, results.size());
        assertEquals("batch-1", results.get(0).getBatchId());
    }

    @Test
    @DisplayName("Should calculate batch statistics")
    void shouldCalculateBatchStatistics() {
        // 2 valid batches (5 messages, same partition)
        // 1 invalid batch (3 messages)
        service.recordBatch(createBatch("b1", 0, 5));
        service.recordBatch(createBatch("b2", 1, 5));
        service.recordBatch(createBatch("b3", 2, 3));

        var stats = service.getStatistics();

        assertEquals(3, stats.getTotalBatches());
        assertEquals(2, stats.getValidBatches()); // 5 messages = valid
        assertEquals(3, stats.getSamePartitionBatches());
        assertEquals(2, stats.getBatchesWithFiveMessages());
    }

    @Test
    @DisplayName("Should track mixed partition batches")
    void shouldTrackMixedPartitionBatches() {
        // Batch with mixed partitions
        BatchAnalysisResult mixedBatch = BatchAnalysisResult.builder()
                .batchId("mixed")
                .receivedAt(Instant.now())
                .consumerThread("thread-0")
                .messageCount(5)
                .partitionsInBatch(Set.of(0, 1)) // Mixed!
                .allMessagesFromSamePartition(false)
                .messageIds(List.of("msg1", "msg2"))
                .summary("Mixed batch")
                .build();

        service.recordBatch(mixedBatch);

        var stats = service.getStatistics();
        assertEquals(1, stats.getMixedPartitionBatches());
        assertEquals(0, stats.getSamePartitionBatches());
    }

    @Test
    @DisplayName("Should track batch size distribution")
    void shouldTrackBatchSizeDistribution() {
        service.recordBatch(createBatch("b1", 0, 5));
        service.recordBatch(createBatch("b2", 0, 5));
        service.recordBatch(createBatch("b3", 0, 3));
        service.recordBatch(createBatch("b4", 0, 1));

        var stats = service.getStatistics();
        var distribution = stats.getBatchSizeDistribution();

        assertEquals(2, distribution.get(5)); // Two batches with 5 messages
        assertEquals(1, distribution.get(3)); // One batch with 3 messages
        assertEquals(1, distribution.get(1)); // One batch with 1 message
    }

    @Test
    @DisplayName("Should track thread distribution")
    void shouldTrackThreadDistribution() {
        service.recordBatch(createBatch("b1", 0, 5));
        service.recordBatch(createBatch("b2", 0, 5));
        service.recordBatch(createBatch("b3", 1, 5));

        var stats = service.getStatistics();
        var threadDist = stats.getThreadDistribution();

        assertEquals(2, threadDist.get("thread-0"));
        assertEquals(1, threadDist.get("thread-1"));
    }

    @Test
    @DisplayName("Should clear all results")
    void shouldClearAllResults() {
        service.recordBatch(createBatch("b1", 0, 5));
        service.recordBatch(createBatch("b2", 1, 5));

        service.clearResults();

        assertTrue(service.getAllResults().isEmpty());
        assertEquals(0, service.getStatistics().getTotalBatches());
    }

    @Test
    @DisplayName("Should handle concurrent writes with virtual threads")
    void shouldHandleConcurrentWritesWithVirtualThreads() throws InterruptedException {
        int numThreads = 100;
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {
            final int index = i;
            Thread.startVirtualThread(() -> {
                try {
                    service.recordBatch(createBatch("batch-" + index, index % 3, 5));
                    successCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        boolean completed = doneLatch.await(10, TimeUnit.SECONDS);
        assertTrue(completed, "All virtual threads should complete");
        assertEquals(numThreads, successCount.get());
        assertEquals(numThreads, service.getAllResults().size());
    }

    @Test
    @DisplayName("Should handle concurrent reads and writes with virtual threads")
    void shouldHandleConcurrentReadsAndWritesWithVirtualThreads() throws InterruptedException {
        int numWriters = 50;
        int numReaders = 50;
        CountDownLatch doneLatch = new CountDownLatch(numWriters + numReaders);
        AtomicInteger errorCount = new AtomicInteger(0);

        // Writers
        for (int i = 0; i < numWriters; i++) {
            final int index = i;
            Thread.startVirtualThread(() -> {
                try {
                    service.recordBatch(createBatch("batch-" + index, 0, 5));
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Readers
        for (int i = 0; i < numReaders; i++) {
            Thread.startVirtualThread(() -> {
                try {
                    service.getAllResults();
                    service.getStatistics();
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        boolean completed = doneLatch.await(10, TimeUnit.SECONDS);
        assertTrue(completed, "All virtual threads should complete");
        assertEquals(0, errorCount.get(), "No errors should occur during concurrent access");
    }
}
