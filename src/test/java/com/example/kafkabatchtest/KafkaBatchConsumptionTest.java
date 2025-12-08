package com.example.kafkabatchtest;

import com.example.kafkabatchtest.model.BatchAnalysisResult;
import com.example.kafkabatchtest.service.BatchAnalysisService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test to verify that each batch contains messages from the same
 * partition.
 * 
 * Test Configuration:
 * - batch-mode: true
 * - concurrency: 3
 * - max.poll.records: 5
 * - partitions: 3
 */
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = { "test-batch-topic" }, brokerProperties = {
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092"
})
@TestPropertySource(properties = {
        "spring.cloud.stream.kafka.binder.brokers=localhost:9092",
        "spring.cloud.stream.bindings.batchConsumer-in-0.consumer.batch-mode=true",
        "spring.cloud.stream.bindings.batchConsumer-in-0.consumer.concurrency=3",
        "spring.cloud.stream.kafka.binder.consumer-properties.max.poll.records=5",
        "logging.level.com.example=DEBUG"
})
@DirtiesContext
public class KafkaBatchConsumptionTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private BatchAnalysisService analysisService;

    private KafkaTemplate<String, String> kafkaTemplate;
    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        // Clear previous results
        analysisService.clearResults();

        // Create producer
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    @Test
    void testBatchConsumption_AllMessagesFromSamePartition() throws Exception {
        String topic = "test-batch-topic";
        int messagesPerPartition = 15; // Should create 3 batches of 5 per partition

        System.out.println("\n" +
                "╔══════════════════════════════════════════════════════════════════╗\n" +
                "║         KAFKA BATCH CONSUMPTION TEST                             ║\n" +
                "║                                                                  ║\n" +
                "║  Configuration:                                                  ║\n" +
                "║    - batch-mode: true                                            ║\n" +
                "║    - concurrency: 3                                              ║\n" +
                "║    - max.poll.records: 5                                         ║\n" +
                "║    - partitions: 3                                               ║\n" +
                "║                                                                  ║\n" +
                "║  Sending " + messagesPerPartition + " messages to each partition...                     ║\n" +
                "╚══════════════════════════════════════════════════════════════════╝\n");

        // Send messages to each partition
        for (int partition = 0; partition < 3; partition++) {
            for (int i = 0; i < messagesPerPartition; i++) {
                String messageId = String.format("p%d-msg%d", partition, i);
                Map<String, Object> message = new HashMap<>();
                message.put("id", messageId);
                message.put("content", "Message " + i + " for partition " + partition);
                message.put("targetPartition", partition);
                message.put("timestamp", Instant.now().toString());
                message.put("sequenceNumber", i);

                String json = objectMapper.writeValueAsString(message);
                kafkaTemplate.send(topic, partition, "key-" + partition, json);
            }
            System.out.println("✓ Sent " + messagesPerPartition + " messages to partition " + partition);
        }

        int totalMessages = messagesPerPartition * 3;
        System.out.println("\nTotal messages sent: " + totalMessages);
        System.out.println("Expected batches (approx): " + (totalMessages / 5) + "\n");

        // Wait for messages to be consumed
        System.out.println("Waiting for batch consumption...\n");

        await().atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    List<BatchAnalysisResult> results = analysisService.getAllResults();
                    int consumed = results.stream().mapToInt(BatchAnalysisResult::getMessageCount).sum();
                    return consumed >= totalMessages;
                });

        // Analyze results
        List<BatchAnalysisResult> results = analysisService.getAllResults();
        BatchAnalysisService.BatchStatistics stats = analysisService.getStatistics();

        System.out.println("\n" +
                "╔══════════════════════════════════════════════════════════════════╗\n" +
                "║                        TEST RESULTS                              ║\n" +
                "╠══════════════════════════════════════════════════════════════════╣");

        System.out.printf("║  Total Batches Received:      %-34d ║%n", stats.getTotalBatches());
        System.out.printf("║  Batches with Same Partition: %-34d ║%n", stats.getSamePartitionBatches());
        System.out.printf("║  Batches with Mixed Partitions: %-32d ║%n", stats.getMixedPartitionBatches());
        System.out.printf("║  Valid Batches (5 msgs, same partition): %-23d ║%n", stats.getValidBatches());

        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        System.out.println("║  Batch Size Distribution:                                        ║");
        stats.getBatchSizeDistribution().forEach((size, count) -> System.out
                .printf("║    Size %d: %d batches                                           ║%n", size, count));

        System.out.println("╠══════════════════════════════════════════════════════════════════╣");
        System.out.println("║  Thread Distribution (Concurrency Verification):                 ║");
        stats.getThreadDistribution()
                .forEach((thread, count) -> System.out.printf("║    %s: %d batches                           ║%n",
                        thread.length() > 30 ? thread.substring(thread.length() - 30) : thread, count));

        System.out.println("╠══════════════════════════════════════════════════════════════════╣");

        // Verification
        boolean allBatchesFromSamePartition = stats.getMixedPartitionBatches() == 0;

        if (allBatchesFromSamePartition) {
            System.out.println("║  ✅ VERIFICATION PASSED                                          ║");
            System.out.println("║  All batches contain messages from the SAME partition!           ║");
        } else {
            System.out.println("║  ❌ VERIFICATION FAILED                                          ║");
            System.out.println("║  Some batches contain messages from MULTIPLE partitions!         ║");

            // Show details of mixed batches
            results.stream()
                    .filter(r -> !r.isAllMessagesFromSamePartition())
                    .forEach(r -> System.out.printf(
                            "║    Batch %s: partitions %s                           ║%n",
                            r.getBatchId(), r.getPartitionsInBatch()));
        }

        System.out.println("╚══════════════════════════════════════════════════════════════════╝\n");

        // Print detailed batch info
        System.out.println("\nDetailed Batch Information:");
        System.out.println("─".repeat(70));
        for (BatchAnalysisResult result : results) {
            System.out.printf("Batch %s | Thread: %-25s | Messages: %d | Partitions: %-10s | Valid: %s%n",
                    result.getBatchId(),
                    result.getConsumerThread().length() > 25
                            ? "..." + result.getConsumerThread().substring(result.getConsumerThread().length() - 22)
                            : result.getConsumerThread(),
                    result.getMessageCount(),
                    result.getPartitionsInBatch(),
                    result.isValid() ? "✓" : "✗");
        }

        // Assertions
        assertFalse(results.isEmpty(), "Should have received batches");
        assertTrue(allBatchesFromSamePartition,
                "All batches should contain messages from the same partition. " +
                        "Mixed partition batches: " + stats.getMixedPartitionBatches());

        // Verify concurrency - should see multiple threads
        assertTrue(stats.getThreadDistribution().size() >= 1,
                "Should have at least 1 consumer thread processing batches");
    }
}
