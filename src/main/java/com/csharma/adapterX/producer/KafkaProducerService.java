package com.csharma.adapterX.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.retry.max.attempts:3}")
    private int maxRetryAttempts;

    @Value("${kafka.retry.initial.interval:1000}")
    private long initialInterval;

    @Value("${kafka.retry.multiplier:2.0}")
    private double multiplier;

    @Value("${kafka.retry.max.interval:10000}")
    private long maxInterval;

    @Value("${kafka.dlq.topic}")
    private String dlqTopic;

    /**
     * Sends a message to Kafka topic with retry capability
     * If all retries are exhausted, the message is sent to DLQ
     *
     * @param topic The topic to send the message to
     * @param key The key for the message (can be null)
     * @param payload The message payload
     * @return CompletableFuture<SendResult<String, Object>> The result of the send operation
     */
    public CompletableFuture<SendResult<String, Object>> sendMessageWithRetry(String topic, String key, Object payload) {
        logger.info("Attempting to send message to topic {} with key {}", topic, key);

        RetryTemplate retryTemplate = createRetryTemplate();
        CompletableFuture<SendResult<String, Object>> resultFuture = new CompletableFuture<>();

        try {
            retryTemplate.execute(context -> {
                CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, payload);

                future.whenComplete((result, ex) -> {
                    if (ex != null) {
                        int retryCount = context.getRetryCount();
                        logger.warn("Error sending message to topic {} (attempt: {}): {}",
                                topic, retryCount + 1, ex.getMessage());

                        if (retryCount >= maxRetryAttempts - 1) {
                            // This is the last retry, so we won't throw an exception
                            // The RetryTemplate will exit and we'll handle it below
                            logger.error("Failed to send message after {} attempts", maxRetryAttempts);
                        } else {
                            throw new RuntimeException("Failed to send message", ex);
                        }
                    } else {
                        logger.info("Message sent successfully to topic {} partition {} offset {}",
                                topic, result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                        resultFuture.complete(result);
                    }
                });

                return future;
            }, context -> {
                // Recovery callback - send to DLQ
                logger.info("Sending failed message to DLQ topic {}", dlqTopic);

                // Create a producer record to add headers with original topic and error info
                ProducerRecord<String, Object> record = new ProducerRecord<>(dlqTopic, key, payload);
                record.headers().add(new RecordHeader("original-topic", topic.getBytes(StandardCharsets.UTF_8)));
                record.headers().add(new RecordHeader("exception",
                        context.getLastThrowable().getMessage().getBytes(StandardCharsets.UTF_8)));

                CompletableFuture<SendResult<String, Object>> dlqFuture = kafkaTemplate.send(record);

                dlqFuture.whenComplete((result, ex) -> {
                    if (ex != null) {
                        logger.error("Failed to send message to DLQ: {}", ex.getMessage());
                        resultFuture.completeExceptionally(ex);
                    } else {
                        logger.info("Message sent to DLQ successfully at offset {}",
                                result.getRecordMetadata().offset());
                        resultFuture.complete(result);
                    }
                });

                return null;
            });
        } catch (Exception e) {
            logger.error("Unexpected error in retry mechanism: {}", e.getMessage());
            resultFuture.completeExceptionally(e);
        }

        return resultFuture;
    }

    private RetryTemplate createRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        // Configure retry policy
        Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        retryableExceptions.put(Exception.class, true);
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(maxRetryAttempts, retryableExceptions);

        // Configure backoff policy
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(initialInterval);
        backOffPolicy.setMultiplier(multiplier);
        backOffPolicy.setMaxInterval(maxInterval);

        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        // Add retry listener for logging
        retryTemplate.registerListener(new RetryListener() {
            @Override
            public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
                return true;
            }

            @Override
            public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
                // Nothing to do here
            }

            @Override
            public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
                logger.warn("Retry attempt {} failed: {}", context.getRetryCount(), throwable.getMessage());
            }
        });

        return retryTemplate;
    }
}