package com.csharma.adapterX.producer;


import com.csharma.adapterX.producer.properties.KafkaRetryProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Slf4j
@Service
public class KafkaConsumerService {

    private final KafkaRetryProperties properties;
    private final ApplicationContext context;

    public KafkaConsumerService(final KafkaRetryProperties properties, final ApplicationContext context) {
        this.properties = properties;
        this.context = context;
    }

    @KafkaListener(topics = "test-topic")
    public void listen(final ConsumerRecord<String, String> record) {
        try {
          log.info("Processing the consumer record");
        } catch (Exception ex) {
            boolean isRetryable = properties.getConsumer().getRetryable().stream()
                    .anyMatch(className -> ex.getClass().getName().equals(className));

            if (!isRetryable) {
                KafkaRecoveryStrategy strategy = context.getBean(
                        properties.getConsumer().getRecoveryStrategy().toUpperCase(),
                        KafkaRecoveryStrategy.class
                );
                strategy.recover(record.topic(), record.key(), record.value(), ex);
            } else {
                throw ex;
            }
        }
    }
}