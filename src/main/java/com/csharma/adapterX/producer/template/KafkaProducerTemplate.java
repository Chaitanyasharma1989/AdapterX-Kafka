package com.csharma.adapterX.producer.template;

import com.csharma.adapterX.producer.exception.KafkaProducerException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;


@Slf4j
@Service
public class KafkaProducerTemplate <T>{

    @Autowired
    private KafkaTemplate<String, T> kafkaTemplate;

    public CompletableFuture<SendResult<String, T>> send(String topic, T message) {
        log.debug("Sending message to topic {}: {}", topic, message);
        return kafkaTemplate.send(topic, message);
    }

    public CompletableFuture<SendResult<String, T>> send(String topic, String key, T message) {
        log.debug("Sending keyed message to topic {}, key {}: {}", topic, key, message);
        return kafkaTemplate.send(topic, key, message);
    }

    public void sendSync(String topic, T message) throws KafkaProducerException {
        try {
            send(topic, message).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaProducerException("Failed to send message synchronously", e);
        }
    }

    public void sendSync(String topic, String key, T message) throws KafkaProducerException {
        try {
            send(topic, key, message).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new KafkaProducerException("Failed to send keyed message synchronously", e);
        }
    }
}
