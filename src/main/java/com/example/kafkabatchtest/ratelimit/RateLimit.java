package com.example.kafkabatchtest.ratelimit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to apply rate limiting to a method.
 * 
 * The annotated method must have a parameter that provides the partition ID.
 * Use {@code partitionParam} to specify which parameter contains the partition
 * info.
 * 
 * Example usage:
 * 
 * <pre>
 * {@code @RateLimit(partitionParam = "batch.partition")}
 * public ApiCallResult sendBatch(ApiRequestBatch batch) {
 *     // Method implementation
 * }
 * </pre>
 * 
 * The rate limiter will:
 * - Block if the rate limit is exceeded (never rejects)
 * - Store wait time in a ThreadLocal for the method to access
 * - Use partition-specific rate limiting
 * 
 * Virtual Thread Compatible:
 * - Uses ReentrantLock internally (no pinning)
 * - Thread.sleep() during rate limit wait allows virtual threads to unmount
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RateLimit {

    /**
     * SpEL expression to extract the partition ID from method parameters.
     * 
     * Examples:
     * - "batch.partition" - calls batch.getPartition()
     * - "#partition" - uses a parameter named 'partition'
     * - "#args[0].partition" - first arg's partition field
     * 
     * @return SpEL expression for partition extraction
     */
    String partitionParam() default "";

    /**
     * Optional: specify the partition directly if it's a constant.
     * If set to a non-negative value, this takes precedence over partitionParam.
     * 
     * @return fixed partition ID or -1 to use partitionParam
     */
    int partition() default -1;
}
