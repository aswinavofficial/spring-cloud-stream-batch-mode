package com.example.kafkabatchtest.ratelimit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for SlidingWindowRateLimiter.
 * 
 * Tests verify:
 * - Basic rate limiting functionality
 * - Thread safety with ReentrantLock (virtual thread compatible)
 * - Concurrent access from multiple threads
 * - Statistics tracking
 */
class SlidingWindowRateLimiterTest {

    private SlidingWindowRateLimiter rateLimiter;

    @BeforeEach
    void setUp() {
        // 5 requests per second for testing
        rateLimiter = new SlidingWindowRateLimiter("test-partition", 5, 1000);
    }

    @Nested
    @DisplayName("Basic Rate Limiting")
    class BasicRateLimiting {

        @Test
        @DisplayName("Should allow requests within rate limit")
        void shouldAllowRequestsWithinLimit() {
            // First 5 requests should be allowed immediately
            for (int i = 0; i < 5; i++) {
                long waitTime = rateLimiter.tryAcquire();
                assertEquals(0, waitTime, "Request " + (i + 1) + " should be allowed immediately");
            }
        }

        @Test
        @DisplayName("Should return wait time when rate limit exceeded")
        void shouldReturnWaitTimeWhenLimitExceeded() {
            // Fill up the rate limit
            for (int i = 0; i < 5; i++) {
                rateLimiter.tryAcquire();
            }

            // Next request should return a wait time
            long waitTime = rateLimiter.tryAcquire();
            assertTrue(waitTime > 0, "Should return positive wait time when limit exceeded");
            assertTrue(waitTime <= 1000, "Wait time should not exceed window size");
        }

        @Test
        @DisplayName("Should allow requests after window expires")
        void shouldAllowRequestsAfterWindowExpires() throws InterruptedException {
            // Fill up the rate limit
            for (int i = 0; i < 5; i++) {
                rateLimiter.tryAcquire();
            }

            // Wait for window to expire
            Thread.sleep(1100);

            // Should allow new requests
            long waitTime = rateLimiter.tryAcquire();
            assertEquals(0, waitTime, "Should allow requests after window expires");
        }

        @Test
        @DisplayName("Acquire should block and return total wait time")
        void acquireShouldBlockAndReturnWaitTime() throws InterruptedException {
            // Create a rate limiter with very short window for faster test
            SlidingWindowRateLimiter fastLimiter = new SlidingWindowRateLimiter("fast", 2, 200);

            // Fill the limit
            fastLimiter.tryAcquire();
            fastLimiter.tryAcquire();

            // Acquire should block and return wait time
            long startTime = System.currentTimeMillis();
            long waitTime = fastLimiter.acquire();
            long elapsed = System.currentTimeMillis() - startTime;

            assertTrue(waitTime > 0, "Should have waited");
            assertTrue(elapsed >= waitTime - 50, "Should have actually waited the reported time");
        }
    }

    @Nested
    @DisplayName("Thread Safety with ReentrantLock")
    class ThreadSafety {

        @Test
        @DisplayName("Should handle concurrent requests safely")
        void shouldHandleConcurrentRequestsSafely() throws InterruptedException {
            int numThreads = 20;
            int requestsPerThread = 10;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(numThreads);
            AtomicInteger allowedCount = new AtomicInteger(0);
            AtomicInteger delayedCount = new AtomicInteger(0);

            // Create a rate limiter with higher limit for this test
            SlidingWindowRateLimiter concurrentLimiter = new SlidingWindowRateLimiter("concurrent", 50, 1000);

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            for (int i = 0; i < numThreads; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await(); // Wait for all threads to be ready
                        for (int j = 0; j < requestsPerThread; j++) {
                            long waitTime = concurrentLimiter.tryAcquire();
                            if (waitTime == 0) {
                                allowedCount.incrementAndGet();
                            } else {
                                delayedCount.incrementAndGet();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown(); // Start all threads
            boolean completed = doneLatch.await(10, TimeUnit.SECONDS);

            executor.shutdown();
            assertTrue(completed, "All threads should complete");

            // Verify total requests match
            int totalRequests = numThreads * requestsPerThread;
            assertEquals(totalRequests, allowedCount.get() + delayedCount.get(),
                    "Total requests should match allowed + delayed");

            // Get stats and verify consistency
            SlidingWindowRateLimiter.RateLimitStats stats = concurrentLimiter.getStats();
            assertEquals(totalRequests, stats.getTotalRequests(), "Stats should track all requests");
        }

        @Test
        @DisplayName("Should work correctly with virtual threads")
        void shouldWorkWithVirtualThreads() throws InterruptedException {
            int numVirtualThreads = 100;
            CountDownLatch doneLatch = new CountDownLatch(numVirtualThreads);
            AtomicInteger successCount = new AtomicInteger(0);
            List<Throwable> errors = new ArrayList<>();

            SlidingWindowRateLimiter vtLimiter = new SlidingWindowRateLimiter("virtual-threads", 20, 1000);

            // Use virtual threads (Java 21+)
            for (int i = 0; i < numVirtualThreads; i++) {
                Thread.startVirtualThread(() -> {
                    try {
                        vtLimiter.tryAcquire();
                        successCount.incrementAndGet();
                    } catch (Throwable t) {
                        synchronized (errors) {
                            errors.add(t);
                        }
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            boolean completed = doneLatch.await(10, TimeUnit.SECONDS);
            assertTrue(completed, "All virtual threads should complete");
            assertTrue(errors.isEmpty(), "No errors should occur: " + errors);
            assertEquals(numVirtualThreads, successCount.get(), "All requests should be processed");
        }

        @Test
        @DisplayName("Virtual threads should not pin when blocking on acquire")
        void virtualThreadsShouldNotPinOnAcquire() throws InterruptedException {
            // This test verifies that virtual threads can acquire and release properly
            // without carrier thread pinning (which would cause deadlocks at scale)

            int numVirtualThreads = 50;
            CountDownLatch doneLatch = new CountDownLatch(numVirtualThreads);
            AtomicInteger completedCount = new AtomicInteger(0);

            // Very restrictive limiter to force waiting
            SlidingWindowRateLimiter restrictiveLimiter = new SlidingWindowRateLimiter("restrictive", 5, 500);

            for (int i = 0; i < numVirtualThreads; i++) {
                Thread.startVirtualThread(() -> {
                    try {
                        // This will block for most threads
                        restrictiveLimiter.acquire();
                        completedCount.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            // If virtual threads were pinning, this would timeout due to carrier exhaustion
            boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
            assertTrue(completed, "All virtual threads should complete without pinning");
            assertEquals(numVirtualThreads, completedCount.get(), "All requests should complete");
        }
    }

    @Nested
    @DisplayName("Statistics")
    class Statistics {

        @Test
        @DisplayName("Should track allowed requests")
        void shouldTrackAllowedRequests() {
            for (int i = 0; i < 3; i++) {
                rateLimiter.tryAcquire();
            }

            SlidingWindowRateLimiter.RateLimitStats stats = rateLimiter.getStats();
            assertEquals(3, stats.getTotalRequests());
            assertEquals(3, stats.getAllowedRequests());
            assertEquals(0, stats.getDelayedRequests());
        }

        @Test
        @DisplayName("Should track delayed requests")
        void shouldTrackDelayedRequests() {
            // Fill limit
            for (int i = 0; i < 5; i++) {
                rateLimiter.tryAcquire();
            }

            // These should be delayed
            for (int i = 0; i < 3; i++) {
                rateLimiter.tryAcquire();
            }

            SlidingWindowRateLimiter.RateLimitStats stats = rateLimiter.getStats();
            assertEquals(8, stats.getTotalRequests());
            assertEquals(5, stats.getAllowedRequests());
            assertEquals(3, stats.getDelayedRequests());
        }

        @Test
        @DisplayName("Should calculate delayed percentage correctly")
        void shouldCalculateDelayedPercentage() {
            // 4 allowed + 1 delayed = 20% delayed
            for (int i = 0; i < 5; i++) {
                rateLimiter.tryAcquire();
            }
            rateLimiter.tryAcquire(); // This will be delayed

            SlidingWindowRateLimiter.RateLimitStats stats = rateLimiter.getStats();
            // 1 delayed out of 6 total = ~16.67%
            assertTrue(stats.getDelayedPercentage() > 16 && stats.getDelayedPercentage() < 17);
        }

        @Test
        @DisplayName("Should reset statistics")
        void shouldResetStatistics() {
            for (int i = 0; i < 5; i++) {
                rateLimiter.tryAcquire();
            }

            rateLimiter.resetStats();
            SlidingWindowRateLimiter.RateLimitStats stats = rateLimiter.getStats();

            assertEquals(0, stats.getTotalRequests());
            assertEquals(0, stats.getAllowedRequests());
            assertEquals(0, stats.getDelayedRequests());
        }
    }

    @Nested
    @DisplayName("Window Count")
    class WindowCount {

        @Test
        @DisplayName("Should track current window count")
        void shouldTrackCurrentWindowCount() {
            assertEquals(0, rateLimiter.getCurrentWindowCount());

            rateLimiter.tryAcquire();
            assertEquals(1, rateLimiter.getCurrentWindowCount());

            rateLimiter.tryAcquire();
            rateLimiter.tryAcquire();
            assertEquals(3, rateLimiter.getCurrentWindowCount());
        }

        @Test
        @DisplayName("Window count should decrease after expiry")
        void windowCountShouldDecreaseAfterExpiry() throws InterruptedException {
            for (int i = 0; i < 3; i++) {
                rateLimiter.tryAcquire();
            }
            assertEquals(3, rateLimiter.getCurrentWindowCount());

            // Wait for window to expire
            Thread.sleep(1100);

            assertEquals(0, rateLimiter.getCurrentWindowCount());
        }
    }
}
