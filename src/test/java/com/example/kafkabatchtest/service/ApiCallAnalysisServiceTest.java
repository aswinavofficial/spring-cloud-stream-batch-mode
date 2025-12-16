package com.example.kafkabatchtest.service;

import com.example.kafkabatchtest.model.ApiCallResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ApiCallAnalysisService.
 * 
 * Verifies thread safety using CopyOnWriteArrayList (virtual thread
 * compatible).
 */
class ApiCallAnalysisServiceTest {

    private ApiCallAnalysisService service;

    @BeforeEach
    void setUp() {
        service = new ApiCallAnalysisService();
    }

    @Test
    @DisplayName("Should record successful API call")
    void shouldRecordSuccessfulApiCall() {
        ApiCallResult result = ApiCallResult.success(
                "batch-1", 0, 5, 200, "OK", 100, false, 0);

        service.recordApiCall(result);

        List<ApiCallResult> results = service.getAllResults();
        assertEquals(1, results.size());
        assertTrue(results.get(0).isSuccess());
    }

    @Test
    @DisplayName("Should record failed API call")
    void shouldRecordFailedApiCall() {
        ApiCallResult result = ApiCallResult.failure(
                "batch-1", 0, 5, "Connection refused", 50, false, 0);

        service.recordApiCall(result);

        var stats = service.getStatistics();
        assertEquals(1, stats.getTotalCalls());
        assertEquals(0, stats.getSuccessfulCalls());
        assertEquals(1, stats.getFailedCalls());
    }

    @Test
    @DisplayName("Should track delayed calls")
    void shouldTrackDelayedCalls() {
        ApiCallResult result = ApiCallResult.success(
                "batch-1", 0, 5, 200, "OK", 150, true, 50);

        service.recordApiCall(result);

        var stats = service.getStatistics();
        assertEquals(1, stats.getDelayedCalls());
        assertEquals(50, stats.getTotalWaitTimeMs());
    }

    @Test
    @DisplayName("Should calculate correct statistics")
    void shouldCalculateCorrectStatistics() {
        // 3 successful, 1 failed, 2 delayed
        service.recordApiCall(ApiCallResult.success("b1", 0, 5, 200, "OK", 100, false, 0));
        service.recordApiCall(ApiCallResult.success("b2", 0, 5, 200, "OK", 100, true, 50));
        service.recordApiCall(ApiCallResult.success("b3", 1, 5, 200, "OK", 100, true, 75));
        service.recordApiCall(ApiCallResult.failure("b4", 1, 5, "Error", 50, false, 0));

        var stats = service.getStatistics();

        assertEquals(4, stats.getTotalCalls());
        assertEquals(3, stats.getSuccessfulCalls());
        assertEquals(1, stats.getFailedCalls());
        assertEquals(2, stats.getDelayedCalls());
        assertEquals(75.0, stats.getSuccessRate());
        assertEquals(50.0, stats.getDelayedPercentage());
    }

    @Test
    @DisplayName("Should track per-partition statistics")
    void shouldTrackPerPartitionStatistics() {
        service.recordApiCall(ApiCallResult.success("b1", 0, 5, 200, "OK", 100, false, 0));
        service.recordApiCall(ApiCallResult.success("b2", 0, 5, 200, "OK", 100, false, 0));
        service.recordApiCall(ApiCallResult.success("b3", 1, 5, 200, "OK", 100, false, 0));

        var stats = service.getStatistics();

        assertEquals(2, stats.getPartitionStats().get(0).getTotalCalls());
        assertEquals(1, stats.getPartitionStats().get(1).getTotalCalls());
    }

    @Test
    @DisplayName("Should clear all results")
    void shouldClearAllResults() {
        service.recordApiCall(ApiCallResult.success("b1", 0, 5, 200, "OK", 100, false, 0));
        service.recordApiCall(ApiCallResult.success("b2", 0, 5, 200, "OK", 100, false, 0));

        service.clearResults();

        assertTrue(service.getAllResults().isEmpty());
        assertEquals(0, service.getStatistics().getTotalCalls());
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
                    service.recordApiCall(ApiCallResult.success(
                            "batch-" + index, index % 3, 5, 200, "OK", 100, false, 0));
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
                    service.recordApiCall(ApiCallResult.success(
                            "batch-" + index, 0, 5, 200, "OK", 100, false, 0));
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
