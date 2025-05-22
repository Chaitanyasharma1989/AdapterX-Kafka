package com.csharma.adapterx.model;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.List;

public record BulkProducerRequest<K, V>(
        List<ProducerRecord<K, V>> records,
        Duration timeout,
        boolean enableTransaction
) {
    public BulkProducerRequest(List<ProducerRecord<K, V>> records) {
        this(records, Duration.ofSeconds(60), false);
    }

    public BulkProducerRequest<K, V> withTimeout(Duration timeout) {
        return new BulkProducerRequest<>(records, timeout, enableTransaction);
    }

    public BulkProducerRequest<K, V> withTransaction() {
        return new BulkProducerRequest<>(records, timeout, true);
    }
}