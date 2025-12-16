package com.example.kafkabatchtest.ratelimit;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RateLimitAspect.
 * 
 * Tests verify:
 * - Rate limiting is applied correctly
 * - SpEL expression evaluation for partition extraction
 * - RateLimitContext is set and cleared properly
 * - Fixed partition values work correctly
 */
@ExtendWith(MockitoExtension.class)
class RateLimitAspectTest {

    @Mock
    private PartitionRateLimiterManager rateLimiterManager;

    @Mock
    private ProceedingJoinPoint joinPoint;

    @Mock
    private MethodSignature methodSignature;

    private RateLimitAspect aspect;

    @BeforeEach
    void setUp() {
        aspect = new RateLimitAspect(rateLimiterManager);
    }

    @Nested
    @DisplayName("Rate Limit Application")
    class RateLimitApplication {

        @Test
        @DisplayName("Should apply rate limiting and proceed with method")
        void shouldApplyRateLimitingAndProceed() throws Throwable {
            // Arrange
            RateLimit rateLimit = createRateLimit(0, "");
            Object expectedResult = "success";

            when(rateLimiterManager.acquire(0)).thenReturn(0L);
            when(joinPoint.proceed()).thenReturn(expectedResult);

            // Act
            Object result = aspect.applyRateLimit(joinPoint, rateLimit);

            // Assert
            assertEquals(expectedResult, result);
            verify(rateLimiterManager).acquire(0);
            verify(joinPoint).proceed();
        }

        @Test
        @DisplayName("Should set RateLimitContext when delayed")
        void shouldSetContextWhenDelayed() throws Throwable {
            // Arrange
            RateLimit rateLimit = createRateLimit(0, "");
            when(rateLimiterManager.acquire(0)).thenReturn(100L);
            when(joinPoint.getSignature()).thenReturn(methodSignature);
            when(methodSignature.getName()).thenReturn("testMethod");
            when(joinPoint.proceed()).thenAnswer(invocation -> {
                // Verify context is set during method execution
                RateLimitContext.RateLimitInfo info = RateLimitContext.get();
                assertEquals(100L, info.waitTimeMs());
                assertTrue(info.wasDelayed());
                return "result";
            });

            // Act
            aspect.applyRateLimit(joinPoint, rateLimit);

            // Assert - context should be cleared after execution
            RateLimitContext.RateLimitInfo infoAfter = RateLimitContext.get();
            assertEquals(0L, infoAfter.waitTimeMs());
            assertFalse(infoAfter.wasDelayed());
        }

        @Test
        @DisplayName("Should set RateLimitContext when not delayed")
        void shouldSetContextWhenNotDelayed() throws Throwable {
            // Arrange
            RateLimit rateLimit = createRateLimit(0, "");
            when(rateLimiterManager.acquire(0)).thenReturn(0L);
            when(joinPoint.proceed()).thenAnswer(invocation -> {
                RateLimitContext.RateLimitInfo info = RateLimitContext.get();
                assertEquals(0L, info.waitTimeMs());
                assertFalse(info.wasDelayed());
                return "result";
            });

            // Act
            aspect.applyRateLimit(joinPoint, rateLimit);

            // Verify
            verify(joinPoint).proceed();
        }

        @Test
        @DisplayName("Should clear context even if method throws exception")
        void shouldClearContextOnException() throws Throwable {
            // Arrange
            RateLimit rateLimit = createRateLimit(0, "");
            when(rateLimiterManager.acquire(0)).thenReturn(0L); // No delay
            when(joinPoint.proceed()).thenThrow(new RuntimeException("Test exception"));

            // Act & Assert
            assertThrows(RuntimeException.class, () -> aspect.applyRateLimit(joinPoint, rateLimit));

            // Context should still be cleared
            RateLimitContext.RateLimitInfo info = RateLimitContext.get();
            assertEquals(0L, info.waitTimeMs());
            assertFalse(info.wasDelayed());
        }
    }

    @Nested
    @DisplayName("Fixed Partition")
    class FixedPartition {

        @Test
        @DisplayName("Should use fixed partition when specified")
        void shouldUseFixedPartition() throws Throwable {
            // Arrange
            RateLimit rateLimit = createRateLimit(5, "");
            when(rateLimiterManager.acquire(5)).thenReturn(0L);
            when(joinPoint.proceed()).thenReturn("result");

            // Act
            aspect.applyRateLimit(joinPoint, rateLimit);

            // Assert
            verify(rateLimiterManager).acquire(5);
        }

        @Test
        @DisplayName("Should use different fixed partitions correctly")
        void shouldUseDifferentFixedPartitions() throws Throwable {
            // Arrange
            when(joinPoint.proceed()).thenReturn("result");

            // Test partition 0
            when(rateLimiterManager.acquire(0)).thenReturn(0L);
            aspect.applyRateLimit(joinPoint, createRateLimit(0, ""));
            verify(rateLimiterManager).acquire(0);

            // Test partition 2
            when(rateLimiterManager.acquire(2)).thenReturn(0L);
            aspect.applyRateLimit(joinPoint, createRateLimit(2, ""));
            verify(rateLimiterManager).acquire(2);
        }
    }

    @Nested
    @DisplayName("SpEL Partition Expression")
    class SpelPartitionExpression {

        @Test
        @DisplayName("Should extract partition using SpEL expression")
        void shouldExtractPartitionUsingSpel() throws Throwable {
            // Arrange
            TestBatch batch = new TestBatch(3);
            RateLimit rateLimit = createRateLimit(-1, "#batch.partition");

            when(joinPoint.getSignature()).thenReturn(methodSignature);
            when(joinPoint.getArgs()).thenReturn(new Object[] { batch });
            when(methodSignature.getMethod()).thenReturn(getTestMethod());
            when(rateLimiterManager.acquire(3)).thenReturn(0L);
            when(joinPoint.proceed()).thenReturn("result");

            // Act
            aspect.applyRateLimit(joinPoint, rateLimit);

            // Assert
            verify(rateLimiterManager).acquire(3);
        }

        @Test
        @DisplayName("Should extract partition using args array SpEL expression")
        void shouldExtractPartitionUsingArgsArray() throws Throwable {
            // Arrange
            TestBatch batch = new TestBatch(7);
            RateLimit rateLimit = createRateLimit(-1, "#args[0].partition");

            when(joinPoint.getSignature()).thenReturn(methodSignature);
            when(joinPoint.getArgs()).thenReturn(new Object[] { batch });
            when(methodSignature.getMethod()).thenReturn(getTestMethod());
            when(rateLimiterManager.acquire(7)).thenReturn(0L);
            when(joinPoint.proceed()).thenReturn("result");

            // Act
            aspect.applyRateLimit(joinPoint, rateLimit);

            // Assert
            verify(rateLimiterManager).acquire(7);
        }

        @Test
        @DisplayName("Should throw exception for empty partitionParam when partition is -1")
        void shouldThrowExceptionForEmptyPartitionParam() {
            // Arrange
            RateLimit rateLimit = createRateLimit(-1, "");

            // Act & Assert
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> aspect.applyRateLimit(joinPoint, rateLimit));

            assertTrue(exception.getMessage().contains("partitionParam"));
        }

        @Test
        @DisplayName("Should throw exception for invalid SpEL expression")
        void shouldThrowExceptionForInvalidSpelExpression() throws NoSuchMethodException {
            // Arrange
            TestBatch batch = new TestBatch(1);
            RateLimit rateLimit = createRateLimit(-1, "#nonexistent.property");

            when(joinPoint.getSignature()).thenReturn(methodSignature);
            when(joinPoint.getArgs()).thenReturn(new Object[] { batch });
            when(methodSignature.getMethod()).thenReturn(getTestMethod());

            // Act & Assert
            assertThrows(IllegalArgumentException.class,
                    () -> aspect.applyRateLimit(joinPoint, rateLimit));
        }
    }

    @Nested
    @DisplayName("Virtual Thread Compatibility")
    class VirtualThreadCompatibility {

        @Test
        @DisplayName("Should work correctly with virtual threads")
        void shouldWorkWithVirtualThreads() throws Throwable {
            int numThreads = 20;
            java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(numThreads);
            java.util.concurrent.atomic.AtomicInteger successCount = new java.util.concurrent.atomic.AtomicInteger(0);
            java.util.concurrent.atomic.AtomicInteger errorCount = new java.util.concurrent.atomic.AtomicInteger(0);

            // Use a real rate limiter for this test
            PartitionRateLimiterManager realManager = new PartitionRateLimiterManager(10, 1000);
            RateLimitAspect realAspect = new RateLimitAspect(realManager);

            for (int i = 0; i < numThreads; i++) {
                final int partition = i % 3;
                Thread.startVirtualThread(() -> {
                    try {
                        ProceedingJoinPoint mockJoinPoint = mock(ProceedingJoinPoint.class);
                        when(mockJoinPoint.proceed()).thenReturn("success");

                        RateLimit rateLimit = createRateLimit(partition, "");
                        realAspect.applyRateLimit(mockJoinPoint, rateLimit);
                        successCount.incrementAndGet();
                    } catch (Throwable e) {
                        errorCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            boolean completed = latch.await(10, java.util.concurrent.TimeUnit.SECONDS);
            assertTrue(completed, "All virtual threads should complete");
            assertEquals(0, errorCount.get(), "No errors should occur");
            assertEquals(numThreads, successCount.get(), "All threads should succeed");
        }
    }

    // Helper methods

    private RateLimit createRateLimit(int partition, String partitionParam) {
        return new RateLimit() {
            @Override
            public Class<? extends java.lang.annotation.Annotation> annotationType() {
                return RateLimit.class;
            }

            @Override
            public String partitionParam() {
                return partitionParam;
            }

            @Override
            public int partition() {
                return partition;
            }
        };
    }

    private Method getTestMethod() throws NoSuchMethodException {
        return TestService.class.getMethod("processWithBatch", TestBatch.class);
    }

    // Test helper classes

    public static class TestBatch {
        private final int partition;

        public TestBatch(int partition) {
            this.partition = partition;
        }

        public int getPartition() {
            return partition;
        }
    }

    public static class TestService {
        public String processWithBatch(TestBatch batch) {
            return "processed";
        }
    }
}
