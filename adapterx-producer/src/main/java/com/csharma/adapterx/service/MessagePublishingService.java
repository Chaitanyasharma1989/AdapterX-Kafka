package com.csharma.adapterx.service;


import com.csharma.adapterx.delegate.KafkaProducerDelegate;
import com.csharma.adapterx.model.BulkProducerRequest;
import com.csharma.adapterx.model.ProducerRequest;
import com.csharma.adapterx.model.ProducerResponse;
import com.csharma.adapterx.model.ProducerType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class MessagePublishingService {

    private final KafkaProducerDelegate producerDelegate;

    public MessagePublishingService(KafkaProducerDelegate producerDelegate) {
        this.producerDelegate = producerDelegate;
    }

    // Standard message publishing
    public CompletableFuture<ProducerResponse<SendResult<String, Object>>> publishMessage(String topic, String key, Object message) {
        ProducerRequest<String, Object> request = new ProducerRequest<>(topic, key, message, ProducerType.STANDARD);
        return producerDelegate.publish(request);
    }

    // Transactional message publishing
    public CompletableFuture<ProducerResponse<SendResult<String, Object>>> publishTransactionalMessage(String topic, String key, Object message) {
        ProducerRequest<String, Object> request = new ProducerRequest<>(topic, key, message, ProducerType.TRANSACTIONAL);
        return producerDelegate.publish(request);
    }

    // Bulk message publishing
    public CompletableFuture<ProducerResponse<List<SendResult<String, Object>>>> publishBulkMessages(List<ProducerRecord<String, Object>> records) {
        BulkProducerRequest<String, Object> request = new BulkProducerRequest<>(records);
        return producerDelegate.publishBulk(request);
    }

    // Request-reply pattern
    public CompletableFuture<ProducerResponse<String>> publishAndWaitForReply(String topic, String key, Object message) {
        ProducerRequest<String, Object> request = new ProducerRequest<>(topic, key, message, ProducerType.REPLY);
        return producerDelegate.publishAndWaitForReply(request, String.class);
    }

}
