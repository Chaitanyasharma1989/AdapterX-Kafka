package com.csharma.adapterX.consumer.errorhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.HashMap;
import java.util.Map;


@Slf4j
public class CustomErrorHandler implements CommonErrorHandler {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final int maxRetries;

    // Track retries manually
    private final Map<String, Integer> retryCounts = new HashMap<>();

    public CustomErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate, int maxRetries) {
        this.kafkaTemplate = kafkaTemplate;
        this.maxRetries = maxRetries;
    }

    @Override
    public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer, MessageListenerContainer container) {
        String recordKey = record.topic() + "-" + record.partition() + "-" + record.offset();
        int currentRetry = retryCounts.getOrDefault(recordKey, 0);

        if (currentRetry < maxRetries) {
            log.warn("Retry attempt {} for record: {}", currentRetry + 1, record);
            retryCounts.put(recordKey, currentRetry + 1);
            consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
            return false;
        } else {
            log.error("Max retries exceeded. Sending record to DLT: {}", record, thrownException);
            kafkaTemplate.send(record.topic() + ".DLT", record.key(), record.value());
            retryCounts.remove(recordKey);
            return true;
        }
    }
}