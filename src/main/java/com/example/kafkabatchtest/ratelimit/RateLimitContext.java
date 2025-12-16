package com.example.kafkabatchtest.ratelimit;

/**
 * Thread-local context holder for rate limit information.
 * 
 * This allows methods annotated with @RateLimit to access
 * the wait time that was incurred during rate limiting.
 * 
 * Virtual Thread Compatible: ThreadLocal works correctly with virtual threads.
 */
public class RateLimitContext {

    private static final ThreadLocal<RateLimitInfo> CONTEXT = new ThreadLocal<>();

    /**
     * Set the rate limit info for the current thread.
     */
    public static void set(long waitTimeMs, boolean wasDelayed) {
        CONTEXT.set(new RateLimitInfo(waitTimeMs, wasDelayed));
    }

    /**
     * Get the rate limit info for the current thread.
     * 
     * @return the rate limit info, or a default (no delay) if not set
     */
    public static RateLimitInfo get() {
        RateLimitInfo info = CONTEXT.get();
        return info != null ? info : new RateLimitInfo(0, false);
    }

    /**
     * Clear the rate limit info for the current thread.
     * Should be called after the method completes.
     */
    public static void clear() {
        CONTEXT.remove();
    }

    /**
     * Record containing rate limit wait information.
     */
    public record RateLimitInfo(long waitTimeMs, boolean wasDelayed) {
    }
}
