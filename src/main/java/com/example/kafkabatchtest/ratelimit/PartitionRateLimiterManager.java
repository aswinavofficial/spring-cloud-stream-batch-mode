package com.example.kafkabatchtest.ratelimit;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manages rate limiters per Kafka partition.
 * Each partition gets its own rate limiter to ensure independent rate limiting.
 */
@Slf4j
@Component
public class PartitionRateLimiterManager {

    private final Map<Integer, SlidingWindowRateLimiter> rateLimiters = new ConcurrentHashMap<>();

    private final int tps;
    private final long windowSizeMs;

    public PartitionRateLimiterManager(
            @Value("${ratelimit.tps:9}") int tps,
            @Value("${ratelimit.window-size-ms:1000}") long windowSizeMs) {
        this.tps = tps;
        this.windowSizeMs = windowSizeMs;
        log.info("PartitionRateLimiterManager initialized with {} TPS, window {}ms", tps, windowSizeMs);
    }

    /**
     * Get or create a rate limiter for a specific partition.
     */
    public SlidingWindowRateLimiter getRateLimiter(int partition) {
        return rateLimiters.computeIfAbsent(partition, p -> {
            log.info("Creating rate limiter for partition {}", p);
            return new SlidingWindowRateLimiter("partition-" + p, tps, windowSizeMs);
        });
    }

    /**
     * Try to acquire a permit for a partition.
     * 
     * @param partition The partition number
     * @return 0 if acquired, or wait time in ms if rate limited
     */
    public long tryAcquire(int partition) {
        return getRateLimiter(partition).tryAcquire();
    }

    /**
     * Acquire a permit for a partition, blocking if necessary.
     * 
     * @param partition The partition number
     * @return The actual wait time in milliseconds
     * @throws InterruptedException if interrupted while waiting
     */
    public long acquire(int partition) throws InterruptedException {
        return getRateLimiter(partition).acquire();
    }

    /**
     * Get statistics for all partitions.
     */
    public RateLimitStatistics getStatistics() {
        Collection<SlidingWindowRateLimiter.RateLimitStats> partitionStats = rateLimiters.values().stream()
                .map(SlidingWindowRateLimiter::getStats)
                .collect(Collectors.toList());

        long totalRequests = partitionStats.stream()
                .mapToLong(SlidingWindowRateLimiter.RateLimitStats::getTotalRequests).sum();
        long allowedRequests = partitionStats.stream()
                .mapToLong(SlidingWindowRateLimiter.RateLimitStats::getAllowedRequests).sum();
        long delayedRequests = partitionStats.stream()
                .mapToLong(SlidingWindowRateLimiter.RateLimitStats::getDelayedRequests).sum();
        long totalWaitTimeMs = partitionStats.stream()
                .mapToLong(SlidingWindowRateLimiter.RateLimitStats::getTotalWaitTimeMs).sum();

        return RateLimitStatistics.builder()
                .tps(tps)
                .windowSizeMs(windowSizeMs)
                .totalPartitions(rateLimiters.size())
                .totalRequests(totalRequests)
                .allowedRequests(allowedRequests)
                .delayedRequests(delayedRequests)
                .totalWaitTimeMs(totalWaitTimeMs)
                .averageWaitTimeMs(delayedRequests > 0 ? (double) totalWaitTimeMs / delayedRequests : 0.0)
                .delayedPercentage(totalRequests > 0 ? (delayedRequests * 100.0 / totalRequests) : 0.0)
                .partitionStats(partitionStats)
                .build();
    }

    /**
     * Reset statistics for all partitions.
     */
    public void resetStatistics() {
        rateLimiters.values().forEach(SlidingWindowRateLimiter::resetStats);
        log.info("Reset statistics for all {} partitions", rateLimiters.size());
    }

    /**
     * Clear all rate limiters (for testing).
     */
    public void clearAll() {
        rateLimiters.clear();
        log.info("Cleared all rate limiters");
    }
}
