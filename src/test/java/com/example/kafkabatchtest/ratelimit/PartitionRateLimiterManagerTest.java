package com.example.kafkabatchtest.ratelimit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PartitionRateLimiterManager.
 */
class PartitionRateLimiterManagerTest {

    private PartitionRateLimiterManager manager;

    @BeforeEach
    void setUp() {
        manager = new PartitionRateLimiterManager(10, 1000);
    }

    @Test
    @DisplayName("Should create rate limiter for new partition")
    void shouldCreateRateLimiterForNewPartition() {
        SlidingWindowRateLimiter limiter = manager.getRateLimiter(0);
        assertNotNull(limiter);
    }

    @Test
    @DisplayName("Should return same rate limiter for same partition")
    void shouldReturnSameRateLimiterForSamePartition() {
        SlidingWindowRateLimiter limiter1 = manager.getRateLimiter(0);
        SlidingWindowRateLimiter limiter2 = manager.getRateLimiter(0);
        assertSame(limiter1, limiter2);
    }

    @Test
    @DisplayName("Should create different rate limiters for different partitions")
    void shouldCreateDifferentRateLimitersForDifferentPartitions() {
        SlidingWindowRateLimiter limiter0 = manager.getRateLimiter(0);
        SlidingWindowRateLimiter limiter1 = manager.getRateLimiter(1);
        SlidingWindowRateLimiter limiter2 = manager.getRateLimiter(2);

        assertNotSame(limiter0, limiter1);
        assertNotSame(limiter1, limiter2);
        assertNotSame(limiter0, limiter2);
    }

    @Test
    @DisplayName("tryAcquire should delegate to correct partition limiter")
    void tryAcquireShouldDelegateToCorrectPartition() {
        // Fill partition 0's limit
        for (int i = 0; i < 10; i++) {
            manager.tryAcquire(0);
        }

        // Partition 0 should be rate limited
        long waitTime0 = manager.tryAcquire(0);
        assertTrue(waitTime0 > 0, "Partition 0 should be rate limited");

        // Partition 1 should still allow requests
        long waitTime1 = manager.tryAcquire(1);
        assertEquals(0, waitTime1, "Partition 1 should not be rate limited");
    }

    @Test
    @DisplayName("acquire should block and delegate to correct partition limiter")
    void acquireShouldBlockAndDelegate() throws InterruptedException {
        // Create manager with fast window
        PartitionRateLimiterManager fastManager = new PartitionRateLimiterManager(2, 200);

        // Fill the limit for partition 0
        fastManager.tryAcquire(0);
        fastManager.tryAcquire(0);

        // Acquire should block
        long startTime = System.currentTimeMillis();
        long waitTime = fastManager.acquire(0);
        long elapsed = System.currentTimeMillis() - startTime;

        assertTrue(waitTime > 0 || elapsed > 100, "Should have waited");
    }

    @Test
    @DisplayName("Should aggregate statistics from all partitions")
    void shouldAggregateStatisticsFromAllPartitions() {
        // Make requests on multiple partitions
        for (int i = 0; i < 5; i++) {
            manager.tryAcquire(0);
        }
        for (int i = 0; i < 3; i++) {
            manager.tryAcquire(1);
        }
        for (int i = 0; i < 2; i++) {
            manager.tryAcquire(2);
        }

        RateLimitStatistics stats = manager.getStatistics();

        assertEquals(3, stats.getTotalPartitions());
        assertEquals(10, stats.getTotalRequests());
        assertEquals(10, stats.getAllowedRequests());
        assertEquals(0, stats.getDelayedRequests());
    }

    @Test
    @DisplayName("Should reset statistics for all partitions")
    void shouldResetStatisticsForAllPartitions() {
        manager.tryAcquire(0);
        manager.tryAcquire(1);
        manager.tryAcquire(2);

        manager.resetStatistics();

        RateLimitStatistics stats = manager.getStatistics();
        assertEquals(0, stats.getTotalRequests());
    }

    @Test
    @DisplayName("Should clear all rate limiters")
    void shouldClearAllRateLimiters() {
        manager.getRateLimiter(0);
        manager.getRateLimiter(1);
        manager.getRateLimiter(2);

        manager.clearAll();

        RateLimitStatistics stats = manager.getStatistics();
        assertEquals(0, stats.getTotalPartitions());
    }

    @Test
    @DisplayName("Should handle concurrent partition access with virtual threads")
    void shouldHandleConcurrentPartitionAccessWithVirtualThreads() throws InterruptedException {
        int numPartitions = 5;
        int threadsPerPartition = 20;
        CountDownLatch doneLatch = new CountDownLatch(numPartitions * threadsPerPartition);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int p = 0; p < numPartitions; p++) {
            final int partition = p;
            for (int t = 0; t < threadsPerPartition; t++) {
                Thread.startVirtualThread(() -> {
                    try {
                        manager.tryAcquire(partition);
                        successCount.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }
        }

        boolean completed = doneLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(completed, "All virtual threads should complete");
        assertEquals(numPartitions * threadsPerPartition, successCount.get());
    }
}
