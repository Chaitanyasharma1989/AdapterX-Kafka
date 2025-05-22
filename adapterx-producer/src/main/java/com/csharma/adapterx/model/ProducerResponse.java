package com.csharma.adapterx.model;

import org.apache.kafka.clients.producer.RecordMetadata;

public record ProducerResponse<T>(
        boolean success,
        T result,
        Exception error,
        RecordMetadata metadata,
        long processingTimeMs
) {
    public static <T> ProducerResponse<T> success(T result, RecordMetadata metadata, long processingTime) {
        return new ProducerResponse<>(true, result, null, metadata, processingTime);
    }

    public static <T> ProducerResponse<T> failure(Exception error, long processingTime) {
        return new ProducerResponse<>(false, null, error, null, processingTime);
    }
}