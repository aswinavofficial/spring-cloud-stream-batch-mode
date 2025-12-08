package com.example.kafkabatchtest.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Test message model for Kafka consumption testing.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TestMessage {
    
    private String id;
    private String content;
    private Integer targetPartition;
    private Instant timestamp;
    private Integer sequenceNumber;
}
