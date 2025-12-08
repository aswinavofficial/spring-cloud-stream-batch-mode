package com.example.kafkabatchtest.producer;

import com.example.kafkabatchtest.model.TestMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Producer service to send test messages to specific partitions.
 * This allows controlled testing of batch consumption behavior.
 */
@Slf4j
@Service
public class TestMessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final AtomicInteger sequenceCounter = new AtomicInteger(0);

    @Value("${kafka.topic.name:test-batch-topic}")
    private String topicName;

    @Value("${kafka.topic.partitions:3}")
    private int partitionCount;

    public TestMessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    /**
     * Send a batch of messages to a specific partition.
     * 
     * @param partition Target partition (0, 1, or 2)
     * @param count     Number of messages to send
     * @return List of send results
     */
    public List<SendResult<String, String>> sendToPartition(int partition, int count) {
        if (partition < 0 || partition >= partitionCount) {
            throw new IllegalArgumentException("Partition must be between 0 and " + (partitionCount - 1));
        }

        List<CompletableFuture<SendResult<String, String>>> futures = new ArrayList<>();
        String batchId = UUID.randomUUID().toString().substring(0, 8);

        log.info("Sending {} messages to partition {} (batch: {})", count, partition, batchId);

        for (int i = 0; i < count; i++) {
            TestMessage message = TestMessage.builder()
                    .id(batchId + "-" + i)
                    .content("Message " + i + " for partition " + partition)
                    .targetPartition(partition)
                    .timestamp(Instant.now())
                    .sequenceNumber(sequenceCounter.incrementAndGet())
                    .build();

            try {
                String jsonMessage = objectMapper.writeValueAsString(message);
                String key = "partition-" + partition;

                CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, partition, key,
                        jsonMessage);

                futures.add(future);

                log.debug("Sent message {} to partition {}", message.getId(), partition);
            } catch (Exception e) {
                log.error("Failed to send message", e);
            }
        }

        // Wait for all sends to complete
        List<SendResult<String, String>> results = futures.stream()
                .map(CompletableFuture::join)
                .toList();

        log.info("Successfully sent {} messages to partition {}", results.size(), partition);
        return results;
    }

    /**
     * Send messages evenly distributed across all partitions.
     * 
     * @param messagesPerPartition Number of messages per partition
     * @return Total messages sent
     */
    public int sendToAllPartitions(int messagesPerPartition) {
        int totalSent = 0;
        for (int partition = 0; partition < partitionCount; partition++) {
            List<SendResult<String, String>> results = sendToPartition(partition, messagesPerPartition);
            totalSent += results.size();
        }
        log.info("Total messages sent across all {} partitions: {}", partitionCount, totalSent);
        return totalSent;
    }

    /**
     * Send a large number of messages for stress testing.
     * Messages are sent in batches of 5 to each partition in round-robin.
     * 
     * @param totalMessages Total number of messages to send
     * @return Total messages sent
     */
    public int sendStressTest(int totalMessages) {
        int messagesPerPartition = totalMessages / partitionCount;
        int remainder = totalMessages % partitionCount;

        log.info("Starting stress test: {} messages ({} per partition + {} remainder)",
                totalMessages, messagesPerPartition, remainder);

        int totalSent = 0;

        // Send messages in batches of 5 to each partition
        int batchSize = 5;
        int batchesPerPartition = messagesPerPartition / batchSize;

        for (int batch = 0; batch < batchesPerPartition; batch++) {
            for (int partition = 0; partition < partitionCount; partition++) {
                List<SendResult<String, String>> results = sendToPartition(partition, batchSize);
                totalSent += results.size();

                // Small delay between batches to allow consumer to process
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        // Send remaining messages
        for (int partition = 0; partition < partitionCount && remainder > 0; partition++) {
            int toSend = Math.min(remainder, messagesPerPartition % batchSize + 1);
            List<SendResult<String, String>> results = sendToPartition(partition, toSend);
            totalSent += results.size();
            remainder -= toSend;
        }

        log.info("Stress test complete: {} messages sent", totalSent);
        return totalSent;
    }
}
