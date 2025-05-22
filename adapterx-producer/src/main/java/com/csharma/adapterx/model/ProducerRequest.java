package com.csharma.adapterx.model;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public record ProducerRequest<K, V>(
        String topic,
        K key,
        V value,
        ProducerType type,
        Map<String, Object> headers,
        Duration timeout,
        boolean enableTransaction
) {
    public ProducerRequest(String topic, K key, V value, ProducerType type) {
        this(topic, key, value, type, new HashMap<>(), Duration.ofSeconds(30), false);
    }

    public ProducerRequest<K, V> withHeaders(Map<String, Object> headers) {
        return new ProducerRequest<>(topic, key, value, type, headers, timeout, enableTransaction);
    }

    public ProducerRequest<K, V> withTimeout(Duration timeout) {
        return new ProducerRequest<>(topic, key, value, type, headers, timeout, enableTransaction);
    }

    public ProducerRequest<K, V> withTransaction() {
        return new ProducerRequest<>(topic, key, value, type, headers, timeout, true);
    }
}