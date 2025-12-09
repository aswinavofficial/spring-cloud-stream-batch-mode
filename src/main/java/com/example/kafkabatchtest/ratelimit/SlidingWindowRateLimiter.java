package com.example.kafkabatchtest.ratelimit;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe sliding window log rate limiter implementation.
 * 
 * Algorithm: Sliding Window Log
 * - Maintains a log of request timestamps
 * - Removes expired entries (older than window size)
 * - Counts requests in current window
 * - If count >= limit, calculates wait time until oldest entry expires
 * 
 * Thread Safety:
 * - Uses ConcurrentLinkedDeque for timestamp log
 * - Uses AtomicLong for counters
 * - Synchronized cleanup to prevent race conditions
 */
@Slf4j
public class SlidingWindowRateLimiter {

    private final int maxRequestsPerWindow;
    private final long windowSizeMs;
    private final String partitionId;

    // Thread-safe deque to store request timestamps
    private final ConcurrentLinkedDeque<Long> requestLog = new ConcurrentLinkedDeque<>();

    // Statistics
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong allowedRequests = new AtomicLong(0);
    private final AtomicLong delayedRequests = new AtomicLong(0);
    private final AtomicLong totalWaitTimeMs = new AtomicLong(0);

    /**
     * Create a new sliding window rate limiter.
     *
     * @param partitionId          Identifier for this rate limiter (e.g., partition
     *                             number)
     * @param maxRequestsPerWindow Maximum requests allowed per window (TPS)
     * @param windowSizeMs         Window size in milliseconds
     */
    public SlidingWindowRateLimiter(String partitionId, int maxRequestsPerWindow, long windowSizeMs) {
        this.partitionId = partitionId;
        this.maxRequestsPerWindow = maxRequestsPerWindow;
        this.windowSizeMs = windowSizeMs;
        log.info("Created rate limiter for partition {} with {} TPS, window {}ms",
                partitionId, maxRequestsPerWindow, windowSizeMs);
    }

    /**
     * Try to acquire a permit. If rate limit is exceeded, returns the wait time in
     * milliseconds.
     * Does NOT block - caller must handle waiting.
     *
     * @return 0 if permit acquired, or wait time in ms if rate limited
     */
    public long tryAcquire() {
        long now = System.currentTimeMillis();
        totalRequests.incrementAndGet();

        // Clean up expired entries
        cleanupExpiredEntries(now);

        // Check if we're within rate limit
        int currentCount = requestLog.size();

        if (currentCount < maxRequestsPerWindow) {
            // Within limit - allow request
            requestLog.addLast(now);
            allowedRequests.incrementAndGet();
            log.debug("Partition {}: Request allowed. Current count: {}/{}",
                    partitionId, currentCount + 1, maxRequestsPerWindow);
            return 0;
        } else {
            // At limit - calculate wait time
            Long oldestTimestamp = requestLog.peekFirst();
            if (oldestTimestamp == null) {
                // Edge case: concurrent modification, allow request
                requestLog.addLast(now);
                allowedRequests.incrementAndGet();
                return 0;
            }

            long waitTime = (oldestTimestamp + windowSizeMs) - now;
            if (waitTime <= 0) {
                // Oldest entry just expired, we can proceed
                requestLog.addLast(now);
                allowedRequests.incrementAndGet();
                return 0;
            }

            delayedRequests.incrementAndGet();
            totalWaitTimeMs.addAndGet(waitTime);
            log.debug("Partition {}: Rate limited. Wait time: {}ms. Current count: {}/{}",
                    partitionId, waitTime, currentCount, maxRequestsPerWindow);
            return waitTime;
        }
    }

    /**
     * Acquire a permit, blocking if necessary until the rate limit allows.
     * This method will wait and retry until the request is allowed.
     *
     * @return The actual wait time in milliseconds (0 if no wait was needed)
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public long acquire() throws InterruptedException {
        long totalWait = 0;
        long waitTime;

        while ((waitTime = tryAcquire()) > 0) {
            log.debug("Partition {}: Waiting {}ms for rate limit", partitionId, waitTime);
            Thread.sleep(waitTime);
            totalWait += waitTime;
        }

        return totalWait;
    }

    /**
     * Remove entries older than the window size.
     */
    private synchronized void cleanupExpiredEntries(long now) {
        long windowStart = now - windowSizeMs;

        while (!requestLog.isEmpty()) {
            Long oldest = requestLog.peekFirst();
            if (oldest != null && oldest < windowStart) {
                requestLog.pollFirst();
            } else {
                break;
            }
        }
    }

    /**
     * Get the current number of requests in the window.
     */
    public int getCurrentWindowCount() {
        cleanupExpiredEntries(System.currentTimeMillis());
        return requestLog.size();
    }

    /**
     * Get statistics for this rate limiter.
     */
    public RateLimitStats getStats() {
        return RateLimitStats.builder()
                .partitionId(partitionId)
                .maxRequestsPerWindow(maxRequestsPerWindow)
                .windowSizeMs(windowSizeMs)
                .totalRequests(totalRequests.get())
                .allowedRequests(allowedRequests.get())
                .delayedRequests(delayedRequests.get())
                .totalWaitTimeMs(totalWaitTimeMs.get())
                .currentWindowCount(getCurrentWindowCount())
                .averageWaitTimeMs(delayedRequests.get() > 0
                        ? (double) totalWaitTimeMs.get() / delayedRequests.get()
                        : 0.0)
                .build();
    }

    /**
     * Reset statistics.
     */
    public void resetStats() {
        totalRequests.set(0);
        allowedRequests.set(0);
        delayedRequests.set(0);
        totalWaitTimeMs.set(0);
    }

    /**
     * Statistics data class for rate limiter.
     */
    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class RateLimitStats {
        private String partitionId;
        private int maxRequestsPerWindow;
        private long windowSizeMs;
        private long totalRequests;
        private long allowedRequests;
        private long delayedRequests;
        private long totalWaitTimeMs;
        private int currentWindowCount;
        private double averageWaitTimeMs;

        public double getDelayedPercentage() {
            return totalRequests > 0 ? (delayedRequests * 100.0 / totalRequests) : 0.0;
        }
    }
}
