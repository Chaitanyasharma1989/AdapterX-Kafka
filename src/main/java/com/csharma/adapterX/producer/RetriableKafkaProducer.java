package com.csharma.adapterX.producer;


import com.csharma.adapterX.producer.properties.KafkaRetryProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;


@Slf4j
@Component
@RequiredArgsConstructor
public class RetriableKafkaProducer {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final KafkaRetryProperties properties;
    private final ApplicationContext context;

    public void sendSyncWithRetry(String topic, ProducerRecord<String, Object> producerRecord) {
        int attempt = 0;
        Exception lastException = null;

        while (attempt++ < properties.getProducer().getMaxAttempts()) {
            try {
                kafkaTemplate.send(topic, producerRecord.key(), producerRecord.value()).get();
                return;
            } catch (Exception ex) {
                lastException = ex;
                try {
                    Thread.sleep(properties.getProducer().getBackoff());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        KafkaRecoveryStrategy strategy = context.getBean(properties.getProducer().getRecoveryStrategy().toUpperCase(), KafkaRecoveryStrategy.class);
        strategy.recover((String) producerRecord.value(), new KafkaRecoveryContext("producer", topic, producerRecord.partition(), null, producerRecord.key()), lastException);
    }


    public void sendBatch(String topic, List<ProducerRecord<Object, Object>> records) {
        List<ProducerRecord<Object, Object>> failed = new ArrayList<>();
        for (ProducerRecord<Object, Object> record : records) {
            try {
                sendSyncWithRetry(record.topic(), record.key(), record.value());
            } catch (Exception e) {
                failed.add(record);
            }
        }
        if (!failed.isEmpty()) {
            log.error("Failed to publish {} events after retries", failed.size());
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }

}
