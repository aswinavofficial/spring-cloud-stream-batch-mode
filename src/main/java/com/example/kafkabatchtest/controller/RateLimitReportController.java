package com.example.kafkabatchtest.controller;

import com.example.kafkabatchtest.model.ApiCallResult;
import com.example.kafkabatchtest.ratelimit.PartitionRateLimiterManager;
import com.example.kafkabatchtest.ratelimit.RateLimitStatistics;
import com.example.kafkabatchtest.service.ApiCallAnalysisService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST API for rate limiting and API call reports.
 */
@Slf4j
@RestController
@RequestMapping("/api/ratelimit")
@RequiredArgsConstructor
public class RateLimitReportController {

    private final PartitionRateLimiterManager rateLimiterManager;
    private final ApiCallAnalysisService apiCallAnalysisService;

    /**
     * Get rate limiting statistics.
     */
    @GetMapping("/statistics")
    public ResponseEntity<RateLimitStatistics> getStatistics() {
        return ResponseEntity.ok(rateLimiterManager.getStatistics());
    }

    /**
     * Get API call statistics.
     */
    @GetMapping("/api-calls")
    public ResponseEntity<ApiCallAnalysisService.ApiCallStatistics> getApiCallStatistics() {
        return ResponseEntity.ok(apiCallAnalysisService.getStatistics());
    }

    /**
     * Get all API call results.
     */
    @GetMapping("/api-calls/results")
    public ResponseEntity<List<ApiCallResult>> getApiCallResults() {
        return ResponseEntity.ok(apiCallAnalysisService.getAllResults());
    }

    /**
     * Get comprehensive report.
     */
    @GetMapping("/report")
    public ResponseEntity<Map<String, Object>> getReport() {
        RateLimitStatistics rateLimitStats = rateLimiterManager.getStatistics();
        ApiCallAnalysisService.ApiCallStatistics apiStats = apiCallAnalysisService.getStatistics();

        Map<String, Object> report = new HashMap<>();

        // Configuration
        report.put("configuration", Map.of(
                "tps", rateLimitStats.getTps(),
                "windowSizeMs", rateLimitStats.getWindowSizeMs(),
                "algorithm", "Sliding Window Log",
                "perPartitionRateLimiting", true));

        // Rate limiting summary
        report.put("rateLimitSummary", Map.of(
                "totalRequests", rateLimitStats.getTotalRequests(),
                "allowedRequests", rateLimitStats.getAllowedRequests(),
                "delayedRequests", rateLimitStats.getDelayedRequests(),
                "delayedPercentage", String.format("%.1f%%", rateLimitStats.getDelayedPercentage()),
                "averageWaitTimeMs", String.format("%.1f", rateLimitStats.getAverageWaitTimeMs())));

        // API call summary
        report.put("apiCallSummary", Map.of(
                "totalCalls", apiStats.getTotalCalls(),
                "successfulCalls", apiStats.getSuccessfulCalls(),
                "failedCalls", apiStats.getFailedCalls(),
                "successRate", String.format("%.1f%%", apiStats.getSuccessRate()),
                "totalMessagesProcessed", apiStats.getTotalMessagesProcessed(),
                "averageDurationMs", String.format("%.1f", apiStats.getAverageDurationMs())));

        // Per-partition breakdown
        report.put("partitionStats", apiStats.getPartitionStats());

        // Summaries
        report.put("rateLimitStatsSummary", rateLimitStats.getSummary());
        report.put("apiCallStatsSummary", apiStats.getSummary());

        // Recent API calls (last 10)
        List<ApiCallResult> allResults = apiCallAnalysisService.getAllResults();
        int sampleSize = Math.min(10, allResults.size());
        report.put("recentApiCalls", allResults.subList(
                Math.max(0, allResults.size() - sampleSize),
                allResults.size()));

        return ResponseEntity.ok(report);
    }

    /**
     * Clear all statistics.
     */
    @DeleteMapping("/clear")
    public ResponseEntity<Map<String, String>> clearStatistics() {
        rateLimiterManager.resetStatistics();
        apiCallAnalysisService.clearResults();
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "All rate limit and API call statistics cleared"));
    }

    /**
     * Get info about rate limiting endpoints.
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> info() {
        return ResponseEntity.ok(Map.of(
                "description", "Rate Limiting with Sliding Window Log Algorithm",
                "configuration", Map.of(
                        "tps", rateLimiterManager.getStatistics().getTps(),
                        "windowSizeMs", rateLimiterManager.getStatistics().getWindowSizeMs(),
                        "perPartition", true),
                "endpoints", Map.of(
                        "GET /api/ratelimit/statistics", "Get rate limiting statistics",
                        "GET /api/ratelimit/api-calls", "Get API call statistics",
                        "GET /api/ratelimit/api-calls/results", "Get all API call results",
                        "GET /api/ratelimit/report", "Get comprehensive report",
                        "DELETE /api/ratelimit/clear", "Clear all statistics")));
    }
}
