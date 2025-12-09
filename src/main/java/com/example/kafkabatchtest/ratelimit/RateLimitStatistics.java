package com.example.kafkabatchtest.ratelimit;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collection;

/**
 * Aggregate statistics for rate limiting across all partitions.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitStatistics {

    private int tps;
    private long windowSizeMs;
    private int totalPartitions;
    private long totalRequests;
    private long allowedRequests;
    private long delayedRequests;
    private long totalWaitTimeMs;
    private double averageWaitTimeMs;
    private double delayedPercentage;
    private Collection<SlidingWindowRateLimiter.RateLimitStats> partitionStats;

    public String getSummary() {
        return String.format(
                "Rate Limit Stats: %d TPS | Total: %d | Allowed: %d | Delayed: %d (%.1f%%) | Avg Wait: %.1fms",
                tps, totalRequests, allowedRequests, delayedRequests, delayedPercentage, averageWaitTimeMs);
    }
}
