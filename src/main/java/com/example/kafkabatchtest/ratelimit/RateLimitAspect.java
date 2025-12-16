package com.example.kafkabatchtest.ratelimit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * Aspect that intercepts methods annotated with @RateLimit and applies
 * partition-based rate limiting before method execution.
 * 
 * Features:
 * - Extracts partition ID using SpEL expressions
 * - Blocks if rate limit exceeded (never rejects)
 * - Stores wait time in RateLimitContext for the method to access
 * 
 * Virtual Thread Compatible:
 * - Uses ReentrantLock internally (no carrier thread pinning)
 * - Thread.sleep() allows virtual threads to unmount while waiting
 */
@Aspect
@Component
@Slf4j
@RequiredArgsConstructor
public class RateLimitAspect {

    private final PartitionRateLimiterManager rateLimiterManager;
    private final ExpressionParser expressionParser = new SpelExpressionParser();
    private final ParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();

    @Around("@annotation(rateLimit)")
    public Object applyRateLimit(ProceedingJoinPoint joinPoint, RateLimit rateLimit) throws Throwable {
        int partition = resolvePartition(joinPoint, rateLimit);

        try {
            // Apply rate limiting - will block if limit exceeded
            long waitTimeMs = rateLimiterManager.acquire(partition);
            boolean wasDelayed = waitTimeMs > 0;

            if (wasDelayed) {
                log.debug("Rate limit applied: partition={}, waitTime={}ms, method={}",
                        partition, waitTimeMs, joinPoint.getSignature().getName());
            }

            // Store in context for the method to access
            RateLimitContext.set(waitTimeMs, wasDelayed);

            // Proceed with the actual method call
            return joinPoint.proceed();

        } finally {
            // Clean up the context
            RateLimitContext.clear();
        }
    }

    /**
     * Resolve the partition ID from the annotation configuration.
     */
    private int resolvePartition(ProceedingJoinPoint joinPoint, RateLimit rateLimit) {
        // If a fixed partition is specified, use it
        if (rateLimit.partition() >= 0) {
            return rateLimit.partition();
        }

        // Otherwise, use SpEL to extract from parameters
        String partitionParam = rateLimit.partitionParam();
        if (partitionParam == null || partitionParam.isEmpty()) {
            throw new IllegalArgumentException(
                    "@RateLimit requires either 'partition' or 'partitionParam' to be specified");
        }

        return evaluatePartitionExpression(joinPoint, partitionParam);
    }

    /**
     * Evaluate SpEL expression to extract partition from method arguments.
     */
    private int evaluatePartitionExpression(ProceedingJoinPoint joinPoint, String expression) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] args = joinPoint.getArgs();

        // Create evaluation context with method parameters
        EvaluationContext context = createEvaluationContext(method, args);

        // Handle simple property path (e.g., "batch.partition")
        // Convert to SpEL format if needed
        String spelExpression = expression;
        if (!expression.startsWith("#") && !expression.startsWith("@")) {
            // Assume it's a property path on the first argument
            spelExpression = "#" + expression;
        }

        try {
            Object result = expressionParser.parseExpression(spelExpression).getValue(context);
            if (result instanceof Number) {
                return ((Number) result).intValue();
            }
            throw new IllegalArgumentException(
                    "Partition expression must evaluate to a number, got: " + result);
        } catch (Exception e) {
            log.error("Failed to evaluate partition expression '{}': {}", expression, e.getMessage());
            throw new IllegalArgumentException(
                    "Failed to evaluate partition expression: " + expression, e);
        }
    }

    /**
     * Create SpEL evaluation context with method parameters.
     */
    private EvaluationContext createEvaluationContext(Method method, Object[] args) {
        StandardEvaluationContext context = new StandardEvaluationContext();

        // Discover parameter names
        String[] paramNames = parameterNameDiscoverer.getParameterNames(method);
        if (paramNames != null) {
            for (int i = 0; i < paramNames.length; i++) {
                context.setVariable(paramNames[i], args[i]);
            }
        }

        // Also add args array for positional access
        context.setVariable("args", args);

        return context;
    }
}
