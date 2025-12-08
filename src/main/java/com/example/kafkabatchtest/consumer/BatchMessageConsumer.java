package com.example.kafkabatchtest.consumer;

import com.example.kafkabatchtest.model.BatchAnalysisResult;
import com.example.kafkabatchtest.model.TestMessage;
import com.example.kafkabatchtest.service.BatchAnalysisService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;

/**
 * Batch message consumer that processes messages in batches and analyzes
 * whether all messages in each batch come from the same partition.
 * 
 * Configuration:
 * - batch-mode: true
 * - concurrency: 3
 * - max.poll.records: 5
 * - 3 Kafka partitions
 */
@Slf4j
@Configuration
public class BatchMessageConsumer {

    private final BatchAnalysisService analysisService;
    private final ObjectMapper objectMapper;

    public BatchMessageConsumer(BatchAnalysisService analysisService) {
        this.analysisService = analysisService;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    /**
     * Batch consumer function that receives a list of messages.
     * Each invocation receives a batch of messages from a single poll.
     */
    @Bean
    public Consumer<Message<List<String>>> batchConsumer() {
        return message -> {
            String threadName = Thread.currentThread().getName();
            String batchId = UUID.randomUUID().toString().substring(0, 8);
            Instant receivedAt = Instant.now();

            log.debug("Received batch on thread: {}", threadName);

            // Get the payload (list of message bodies as strings)
            List<String> payloads = message.getPayload();

            // Get headers - these will be lists corresponding to each message
            List<Integer> partitions = getHeaderList(message, KafkaHeaders.RECEIVED_PARTITION);
            List<Long> offsets = getHeaderList(message, KafkaHeaders.OFFSET);

            // If partitions is empty, try the older header name
            if (partitions.isEmpty()) {
                partitions = getHeaderList(message, "kafka_receivedPartitionId");
            }

            // Analyze the batch
            Set<Integer> uniquePartitions = new HashSet<>(partitions);
            boolean allSamePartition = uniquePartitions.size() <= 1;

            // Parse messages
            List<String> messageIds = new ArrayList<>();
            List<TestMessage> parsedMessages = new ArrayList<>();

            for (int i = 0; i < payloads.size(); i++) {
                try {
                    String jsonPayload = payloads.get(i);
                    TestMessage testMessage = objectMapper.readValue(jsonPayload, TestMessage.class);
                    parsedMessages.add(testMessage);
                    messageIds.add(testMessage.getId());

                    log.debug("  Message {}: id={}, partition={}, offset={}",
                            i + 1,
                            testMessage.getId(),
                            partitions.size() > i ? partitions.get(i) : "unknown",
                            offsets.size() > i ? offsets.get(i) : "unknown");
                } catch (Exception e) {
                    messageIds.add("raw-" + i);
                    log.debug("  Message {} (raw): content={}, error={}",
                            i + 1,
                            payloads.get(i).substring(0, Math.min(50, payloads.get(i).length())),
                            e.getMessage());
                }
            }

            // Build summary
            String summary = String.format(
                    "Batch %s: %d messages from partition(s) %s on thread %s",
                    batchId,
                    payloads.size(),
                    uniquePartitions,
                    threadName);

            // Create and record analysis result
            BatchAnalysisResult result = BatchAnalysisResult.builder()
                    .batchId(batchId)
                    .receivedAt(receivedAt)
                    .consumerThread(threadName)
                    .messageCount(payloads.size())
                    .partitionsInBatch(uniquePartitions)
                    .allMessagesFromSamePartition(allSamePartition)
                    .messageIds(messageIds)
                    .summary(summary)
                    .build();

            analysisService.recordBatch(result);

            // Add a small delay to simulate processing
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    /**
     * Extract header values as a list from a batch message.
     */
    @SuppressWarnings("unchecked")
    private <T> List<T> getHeaderList(Message<?> message, String headerName) {
        Object headerValue = message.getHeaders().get(headerName);
        if (headerValue instanceof List) {
            return (List<T>) headerValue;
        } else if (headerValue != null) {
            return Collections.singletonList((T) headerValue);
        }
        return Collections.emptyList();
    }
}
