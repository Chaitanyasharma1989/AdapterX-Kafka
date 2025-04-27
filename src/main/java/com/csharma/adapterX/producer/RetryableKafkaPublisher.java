package com.csharma.adapterX.producer;

import com.csharma.adapterX.producer.exception.KafkaProducerException;
import com.csharma.adapterX.producer.properties.KafkaProducerProperties;
import com.csharma.adapterX.producer.template.KafkaProducerTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import java.util.List;


@Slf4j
@Service
public class RetryableKafkaPublisher <T> {

    @Autowired
    private KafkaProducerTemplate<T> producerTemplate;

    @Autowired
    private KafkaProducerProperties properties;

    private final RetryTemplate retryTemplate;

    public RetryableKafkaPublisher(KafkaProducerProperties properties) {
        this.retryTemplate = createRetryTemplate(properties.getRetry());
    }

    @Retryable(
            retryFor = { KafkaProducerException.class },
            maxAttemptsExpression = "${kafka.producer.retry.max-attempts}",
            backoff = @Backoff(
                    delayExpression = "${kafka.producer.retry.initial-interval}",
                    multiplierExpression = "${kafka.producer.retry.multiplier}",
                    maxDelayExpression = "${kafka.producer.retry.max-interval}"
            )
    )
    public void sendWithRetry(String topic, T message) throws KafkaProducerException {
        producerTemplate.sendSync(topic, message);
    }

    @Retryable(
            retryFor = { KafkaProducerException.class },
            maxAttemptsExpression = "${kafka.producer.retry.max-attempts}",
            backoff = @Backoff(
                    delayExpression = "${kafka.producer.retry.initial-interval}",
                    multiplierExpression = "${kafka.producer.retry.multiplier}",
                    maxDelayExpression = "${kafka.producer.retry.max-interval}"
            )
    )
    public void sendWithRetry(String topic, String key, T message) throws KafkaProducerException {
        producerTemplate.sendSync(topic, key, message);
    }

    public void publishBulkWithRetry(String topic, List<T> messages) {
        log.info("Publishing bulk of {} messages with retry to topic {}", messages.size(), topic);
        for (T message : messages) {
            try {
                retryTemplate.execute(context -> {
                    try {
                        producerTemplate.sendSync(topic, message);
                        return null;
                    } catch (Exception e) {
                        log.warn("Failed to publish message to topic {}, attempt {}: {}", topic, context.getRetryCount(), e.getMessage());
                        throw new KafkaProducerException("Failed to publish message", e);
                    }
                });
            } catch (Exception e) {
                log.error("Failed to publish message after all retries: {}", e.getMessage());
                // Handle failures - dead letter topic, error store, notification, etc.
            }
        }
    }

    private RetryTemplate createRetryTemplate(KafkaProducerProperties.RetryProperties retryProps) {
        RetryTemplate template = new RetryTemplate();

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(retryProps.getInitialInterval());
        backOffPolicy.setMultiplier(retryProps.getMultiplier());
        backOffPolicy.setMaxInterval(retryProps.getMaxInterval());
        template.setBackOffPolicy(backOffPolicy);

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(retryProps.getMaxAttempts());
        template.setRetryPolicy(retryPolicy);

        return template;
    }
}
