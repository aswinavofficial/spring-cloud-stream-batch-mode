package com.example.kafkabatchtest.client;

import com.example.kafkabatchtest.mockapi.MockApiController;
import com.example.kafkabatchtest.model.ApiCallResult;
import com.example.kafkabatchtest.model.ApiRequestBatch;
import com.example.kafkabatchtest.ratelimit.RateLimit;
import com.example.kafkabatchtest.ratelimit.RateLimitContext;
import com.example.kafkabatchtest.service.ApiCallAnalysisService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

/**
 * Client for calling the dependent API with rate limiting.
 * 
 * Features:
 * - Rate limiting per partition using @RateLimit annotation
 * - Batches up to 5 messages per request
 * - Tracks call results and statistics
 * - Waits and retries if rate limited (never rejects)
 * 
 * The @RateLimit annotation applies the sliding window rate limiter
 * based on the batch's partition, blocking if needed but never rejecting.
 */
@Slf4j
@Component
public class DependentApiClient {

        private final ApiCallAnalysisService analysisService;
        private final RestTemplate restTemplate;
        private final String apiUrl;

        public DependentApiClient(
                        ApiCallAnalysisService analysisService,
                        @Value("${dependent-api.url:http://localhost:8080/api/mock/process}") String apiUrl) {
                this.analysisService = analysisService;
                this.restTemplate = new RestTemplate();
                this.apiUrl = apiUrl;
                log.info("DependentApiClient initialized with URL: {}", apiUrl);
        }

        /**
         * Send a batch to the dependent API with rate limiting.
         * This method will wait if rate limited and never reject requests.
         * 
         * The @RateLimit annotation extracts the partition from batch.getPartition()
         * and applies rate limiting before this method executes.
         *
         * @param batch The batch of messages to send
         * @return The API call result
         */
        @RateLimit(partitionParam = "#batch.partition")
        public ApiCallResult sendBatch(ApiRequestBatch batch) {
                long startTime = System.currentTimeMillis();

                // Get rate limit info from the aspect via ThreadLocal context
                RateLimitContext.RateLimitInfo rateLimitInfo = RateLimitContext.get();
                boolean wasDelayed = rateLimitInfo.wasDelayed();
                long waitTime = rateLimitInfo.waitTimeMs();

                if (wasDelayed) {
                        log.info("Partition {}: Batch {} was delayed by {}ms due to rate limiting",
                                        batch.getPartition(), batch.getBatchId(), waitTime);
                }

                try {
                        // Build request
                        MockApiController.ProcessRequest request = MockApiController.ProcessRequest.builder()
                                        .batchId(batch.getBatchId())
                                        .partition(batch.getPartition())
                                        .messageIds(batch.getMessageIds())
                                        .messages(batch.getMessagePayloads())
                                        .build();

                        HttpHeaders headers = new HttpHeaders();
                        headers.setContentType(MediaType.APPLICATION_JSON);
                        HttpEntity<MockApiController.ProcessRequest> entity = new HttpEntity<>(request, headers);

                        // Make API call
                        ResponseEntity<MockApiController.ProcessResponse> response = restTemplate.postForEntity(apiUrl,
                                        entity,
                                        MockApiController.ProcessResponse.class);

                        long durationMs = System.currentTimeMillis() - startTime;

                        // Create result
                        ApiCallResult result = ApiCallResult.success(
                                        batch.getBatchId(),
                                        batch.getPartition(),
                                        batch.getMessageCount(),
                                        response.getStatusCode().value(),
                                        response.getBody() != null ? response.getBody().getMessage() : "OK",
                                        durationMs,
                                        wasDelayed,
                                        waitTime);

                        // Record result
                        analysisService.recordApiCall(result);

                        log.debug("Partition {}: Batch {} sent successfully in {}ms (wait: {}ms)",
                                        batch.getPartition(), batch.getBatchId(), durationMs, waitTime);

                        return result;

                } catch (RestClientException e) {
                        long durationMs = System.currentTimeMillis() - startTime;

                        log.error("Partition {}: Batch {} failed: {}",
                                        batch.getPartition(), batch.getBatchId(), e.getMessage());

                        ApiCallResult result = ApiCallResult.failure(
                                        batch.getBatchId(),
                                        batch.getPartition(),
                                        batch.getMessageCount(),
                                        e.getMessage(),
                                        durationMs,
                                        wasDelayed,
                                        waitTime);

                        // Record failure
                        analysisService.recordApiCall(result);

                        return result;
                }
        }
}
