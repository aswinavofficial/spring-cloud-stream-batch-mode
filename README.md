# Kafka Batch Consumption Test with Rate Limiting

This Spring Boot application tests **Spring Cloud Stream Kafka batch consumption** with **rate-limited dependent API integration**.

## Features

### Kafka Batch Consumption
| Parameter | Value |
|-----------|-------|
| Batch Mode | `true` |
| Concurrency | `3` |
| Max Poll Records | `5` |
| Kafka Partitions | `3` |

### Rate-Limited API Integration
| Parameter | Value |
|-----------|-------|
| Rate Limit | `9 TPS` per partition |
| Algorithm | Sliding Window Log |
| Batch Size | `5` messages per API request |
| Retry Policy | Wait and retry (never reject) |

## Goals

1. Verify whether **each batch of messages contains messages from the same partition**
2. Demonstrate **sliding window rate limiting** at 9 TPS per partition
3. Show that rate-limited requests are **delayed but never rejected**

## Prerequisites

- Java 17+
- Docker (for running Kafka)
- Maven

## Quick Start

### 1. Start Kafka with Docker

```bash
# Start Kafka using Docker Compose
docker-compose up -d
```

Or manually:

```bash
# Start Zookeeper
docker run -d --name zookeeper -p 2181:2181 zookeeper:3.8

# Start Kafka with 3 partitions support
docker run -d --name kafka -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=host.docker.internal:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:7.5.0
```

### 2. Build and Run the Application

```bash
cd kafka-batch-test
mvn clean install
mvn spring-boot:run
```

### 3. Send Test Messages

```bash
# Send 10 messages to partition 0
curl -X POST "http://localhost:8080/api/test/send/0?count=10"

# Send 10 messages to partition 1
curl -X POST "http://localhost:8080/api/test/send/1?count=10"

# Send 10 messages to partition 2
curl -X POST "http://localhost:8080/api/test/send/2?count=10"

# Or send to all partitions at once
curl -X POST "http://localhost:8080/api/test/send/all?messagesPerPartition=10"

# Run stress test with 60 messages
curl -X POST "http://localhost:8080/api/test/stress?totalMessages=60"
```

### 4. View Results

```bash
# Get batch analysis results
curl http://localhost:8080/api/test/results | jq

# Get statistics
curl http://localhost:8080/api/test/statistics | jq

# Get detailed report
curl http://localhost:8080/api/test/report | jq

# Clear results
curl -X DELETE http://localhost:8080/api/test/clear
```

## API Endpoints

### Test Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/test/send/{partition}?count=N` | Send N messages to specific partition |
| POST | `/api/test/send/all?messagesPerPartition=N` | Send N messages to each partition |
| POST | `/api/test/stress?totalMessages=N` | Run stress test with N total messages |
| GET | `/api/test/results` | Get all batch analysis results |
| GET | `/api/test/statistics` | Get consumption statistics |
| GET | `/api/test/report` | Get detailed analysis report |
| DELETE | `/api/test/clear` | Clear all recorded results |
| GET | `/api/test/info` | Get application info and endpoints |

### Rate Limiting Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/ratelimit/statistics` | Get rate limiting statistics |
| GET | `/api/ratelimit/api-calls` | Get API call statistics |
| GET | `/api/ratelimit/api-calls/results` | Get all API call results |
| GET | `/api/ratelimit/report` | Get comprehensive rate limit report |
| DELETE | `/api/ratelimit/clear` | Clear all rate limit statistics |
| GET | `/api/ratelimit/info` | Get rate limiting info |

### Mock API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/mock/process` | Process a batch of messages (internal) |
| GET | `/api/mock/stats` | Get mock API statistics |
| DELETE | `/api/mock/stats` | Reset mock API statistics |

## Understanding the Results

### Batch Analysis Result

Each batch received by the consumer is analyzed:

```json
{
  "batchId": "abc12345",
  "receivedAt": "2024-01-01T12:00:00Z",
  "consumerThread": "batch-consumer-0",
  "messageCount": 5,
  "partitionsInBatch": [0],
  "allMessagesFromSamePartition": true,
  "valid": true
}
```

- **valid = true**: Batch has exactly 5 messages AND all from same partition
- **allMessagesFromSamePartition = true**: All messages in batch are from single partition

### Statistics

```json
{
  "totalBatches": 10,
  "validBatches": 10,
  "samePartitionBatches": 10,
  "mixedPartitionBatches": 0,
  "batchSizeDistribution": { "5": 10 },
  "threadDistribution": {
    "batch-consumer-0": 4,
    "batch-consumer-1": 3,
    "batch-consumer-2": 3
  }
}
```

## Expected Behavior

With `concurrency=3` and `partitions=3`:
- Each consumer thread is assigned one partition
- All messages in a batch should come from the same partition
- Batch size should be at most 5 (max.poll.records)

**Expected**: Each batch contains up to 5 messages from the **same partition**.

## Configuration Details

See `src/main/resources/application.yml` for full configuration.

Key settings:
```yaml
spring.cloud.stream:
  bindings:
    batchConsumer-in-0:
      consumer:
        batch-mode: true
        concurrency: 3
  kafka:
    binder:
      consumer-properties:
        max.poll.records: 5
```

## Troubleshooting

### No messages being consumed?
- Ensure Kafka is running on `localhost:9093` (or update `application.yml` if using different port)
- Check if the topic `test-batch-topic` exists with 3 partitions
- Verify no other consumers are in the same group

### Mixed partition batches?
- This should NOT happen with proper configuration
- May indicate a configuration issue or rebalancing

---

## ✅ Test Verification Results

The following results were obtained from running this application with the specified configuration.

### Key Finding: **All batches contain messages from the SAME partition**

| Metric | Value |
|--------|-------|
| **Total Batches** | 11 |
| **Same Partition Batches** | **11 (100%)** |
| **Mixed Partition Batches** | **0** |
| **Valid Batches (5 msgs + same partition)** | 6 (54.5%) |

### Batch Size Distribution

| Batch Size | Count |
|------------|-------|
| 5 messages | 6 batches ✓ |
| 4 messages | 2 batches |
| 3 messages | 2 batches |
| 1 message | 1 batch |

### Thread Distribution (Concurrency = 3)

| Consumer Thread | Batches Processed | Partition |
|-----------------|-------------------|-----------|
| `container-0-C-1` | 3 batches | partition 0 |
| `container-1-C-1` | 4 batches | partition 1 |
| `container-2-C-1` | 4 batches | partition 2 |

### Sample Batch Output

```
╔══════════════════════════════════════════════════════════════════╗
║                    BATCH #7 RECEIVED                             ║
╠══════════════════════════════════════════════════════════════════╣
║  Thread:          container-1-C-1                                
║  Message Count:   5                                              
║  Partitions:      [1]                                            
║  Same Partition:  ✓ YES                                          
║  Valid (5 msgs, same partition): ✓ VALID                         
╚══════════════════════════════════════════════════════════════════╝
```

### Conclusion

| Question | Answer |
|----------|--------|
| Do all batches contain messages from the same partition? | **YES** ✅ |
| Does max.poll.records=5 limit batch size? | **YES** ✅ |
| Are 3 consumer threads created (concurrency=3)? | **YES** ✅ |
| Is each thread assigned to one partition? | **YES** ✅ |

The configuration works as expected:
- **batch-mode: true** ✓ Messages are delivered in batches
- **concurrency: 3** ✓ 3 consumer threads, each handling one partition
- **max.poll.records: 5** ✓ Batches have at most 5 messages
- **partitions: 3** ✓ Messages correctly distributed across 3 partitions

**Each consumer thread is assigned to one partition, and all messages within a batch come from that same partition. No batch contains messages from multiple partitions.**

---

## 📋 Raw Test Evidence

**Test Run Date**: 2025-12-08T18:08:35+05:30

<details>
<summary>Click to expand full JSON response from /api/test/report</summary>

```json
{
  "summary": "Total Batches: 11 | Valid (5 msgs, same partition): 6 (54.5%) | Same Partition: 11 | Mixed Partitions: 0",
  "configuration": {
    "concurrency": 3,
    "batchMode": true,
    "maxPollRecords": 5,
    "partitions": 3
  },
  "expectationMet": true,
  "analysis": {
    "percentageValid": 54.54545454545455,
    "question": "Do all batches contain messages from the same partition?",
    "answer": "YES - All batches have messages from single partition",
    "batchesWithMixedPartitions": 0
  },
  "statistics": {
    "totalBatches": 11,
    "validBatches": 6,
    "samePartitionBatches": 11,
    "batchesWithFiveMessages": 6,
    "mixedPartitionBatches": 0,
    "batchSizeDistribution": {
      "1": 1,
      "3": 2,
      "4": 2,
      "5": 6
    },
    "threadDistribution": {
      "container-0-C-1": 3,
      "container-1-C-1": 4,
      "container-2-C-1": 4
    }
  },
  "recentBatches": [
    {
      "batchId": "d3cfeb41",
      "receivedAt": "2025-12-08T12:38:34.894179Z",
      "consumerThread": "container-0-C-1",
      "messageCount": 5,
      "partitionsInBatch": [0],
      "allMessagesFromSamePartition": true,
      "messageIds": ["11b43eb0-0", "11b43eb0-1", "11b43eb0-2", "11b43eb0-3", "11b43eb0-4"],
      "valid": true
    },
    {
      "batchId": "14365e20",
      "receivedAt": "2025-12-08T12:38:34.894190Z",
      "consumerThread": "container-1-C-1",
      "messageCount": 1,
      "partitionsInBatch": [1],
      "allMessagesFromSamePartition": true,
      "messageIds": ["6ae72ee4-0"],
      "valid": false
    },
    {
      "batchId": "a32e079a",
      "receivedAt": "2025-12-08T12:38:35.014051Z",
      "consumerThread": "container-1-C-1",
      "messageCount": 5,
      "partitionsInBatch": [1],
      "allMessagesFromSamePartition": true,
      "messageIds": ["6ae72ee4-1", "6ae72ee4-2", "6ae72ee4-3", "6ae72ee4-4", "6ae72ee4-5"],
      "valid": true
    },
    {
      "batchId": "1cb6daa0",
      "receivedAt": "2025-12-08T12:38:35.018532Z",
      "consumerThread": "container-0-C-1",
      "messageCount": 5,
      "partitionsInBatch": [0],
      "allMessagesFromSamePartition": true,
      "messageIds": ["11b43eb0-5", "11b43eb0-6", "11b43eb0-7", "11b43eb0-8", "11b43eb0-9"],
      "valid": true
    },
    {
      "batchId": "11310c4e",
      "receivedAt": "2025-12-08T12:38:35.019563Z",
      "consumerThread": "container-2-C-1",
      "messageCount": 5,
      "partitionsInBatch": [2],
      "allMessagesFromSamePartition": true,
      "messageIds": ["b1ac66a6-3", "b1ac66a6-4", "b1ac66a6-5", "b1ac66a6-6", "b1ac66a6-7"],
      "valid": true
    },
    {
      "batchId": "e1ea8eb0",
      "receivedAt": "2025-12-08T12:38:35.124352Z",
      "consumerThread": "container-2-C-1",
      "messageCount": 4,
      "partitionsInBatch": [2],
      "allMessagesFromSamePartition": true,
      "messageIds": ["b1ac66a6-8", "b1ac66a6-9", "b1ac66a6-10", "b1ac66a6-11"],
      "valid": false
    },
    {
      "batchId": "9391e3d7",
      "receivedAt": "2025-12-08T12:38:35.125343Z",
      "consumerThread": "container-1-C-1",
      "messageCount": 5,
      "partitionsInBatch": [1],
      "allMessagesFromSamePartition": true,
      "messageIds": ["6ae72ee4-6", "6ae72ee4-7", "6ae72ee4-8", "6ae72ee4-9", "6ae72ee4-10"],
      "valid": true
    },
    {
      "batchId": "9dea4425",
      "receivedAt": "2025-12-08T12:38:35.125453Z",
      "consumerThread": "container-0-C-1",
      "messageCount": 5,
      "partitionsInBatch": [0],
      "allMessagesFromSamePartition": true,
      "messageIds": ["11b43eb0-10", "11b43eb0-11", "11b43eb0-12", "11b43eb0-13", "11b43eb0-14"],
      "valid": true
    },
    {
      "batchId": "b730a40b",
      "receivedAt": "2025-12-08T12:38:35.229771Z",
      "consumerThread": "container-1-C-1",
      "messageCount": 4,
      "partitionsInBatch": [1],
      "allMessagesFromSamePartition": true,
      "messageIds": ["6ae72ee4-11", "6ae72ee4-12", "6ae72ee4-13", "6ae72ee4-14"],
      "valid": false
    },
    {
      "batchId": "28f717af",
      "receivedAt": "2025-12-08T12:38:35.229986Z",
      "consumerThread": "container-2-C-1",
      "messageCount": 3,
      "partitionsInBatch": [2],
      "allMessagesFromSamePartition": true,
      "messageIds": ["b1ac66a6-12", "b1ac66a6-13", "b1ac66a6-14"],
      "valid": false
    }
  ]
}
```

</details>

**Key observations from the raw data:**
- All 11 batches have `"allMessagesFromSamePartition": true`
- Each batch's `partitionsInBatch` array contains only ONE partition number
- Thread names show clear partition assignment (container-0 → partition 0, container-1 → partition 1, container-2 → partition 2)

---

## 📊 Rate Limiting Test Results

**Test Run Date**: 2025-12-08T18:43:00+05:30

### Configuration
| Setting | Value |
|---------|-------|
| Rate Limit | 9 TPS per partition |
| Algorithm | Sliding Window Log |
| Window Size | 1000ms |
| Per-Partition | Yes |

### Test Results (150 messages stress test)

```json
{
  "rateLimitSummary": {
    "totalRequests": 64,
    "allowedRequests": 53,
    "delayedRequests": 11,
    "delayedPercentage": "17.2%",
    "averageWaitTimeMs": "131.6"
  },
  "apiCallSummary": {
    "totalCalls": 53,
    "successfulCalls": 53,
    "failedCalls": 0,
    "successRate": "100.0%",
    "totalMessagesProcessed": 150,
    "averageDurationMs": "63.7"
  },
  "partitionStats": {
    "0": { "totalCalls": 19, "delayedCalls": 5, "averageWaitTimeMs": 163.6 },
    "1": { "totalCalls": 16, "delayedCalls": 3, "averageWaitTimeMs": 97.0 },
    "2": { "totalCalls": 18, "delayedCalls": 3, "averageWaitTimeMs": 113.0 }
  }
}
```

### Key Observations

| Metric | Result |
|--------|--------|
| Requests rate limited | 11 (17.2%) ✅ |
| Average wait when limited | 131.6ms |
| Requests rejected | 0 (never rejects) ✅ |
| All messages processed | 150/150 (100%) ✅ |
| Success rate | 100% ✅ |

### Conclusion

The sliding window log rate limiting algorithm successfully:
- ✅ **Limits throughput** to 9 TPS per partition
- ✅ **Never rejects** requests - delays and retries instead
- ✅ **Per-partition isolation** - each partition has independent rate limiting
- ✅ **Thread-safe** implementation using `ConcurrentLinkedDeque`
- ✅ **Accurate statistics** tracking for monitoring and debugging
