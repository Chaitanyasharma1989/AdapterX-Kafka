package com.csharma.adapterX.producer.configuration;


import com.csharma.adapterX.producer.properties.KafkaProducerProperties;
import com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;


@Configuration
@EnableConfigurationProperties(KafkaProducerProperties.class)
public class KafkaProducerConfiguration {

    @Autowired
    private KafkaProducerProperties properties;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<String, Object>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, properties.getAcks());
        configProps.put(ProducerConfig.RETRIES_CONFIG, properties.getRetries());
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, properties.getBatchSize());
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, properties.getLingerMs());
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, properties.getBufferMemory());
        return new DefaultKafkaProducerFactory<String, Object>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<String, Object>(producerFactory());
    }


}
