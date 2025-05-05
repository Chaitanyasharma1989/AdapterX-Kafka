package com.csharma.adapterX.producer;

import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;

public class KafkaHeaderUtils {

    public static RecordHeaders withRetryHeaders(int attempt, String errorMsg) {
        RecordHeaders headers = new RecordHeaders();
        headers.add("retry-attempt", String.valueOf(attempt).getBytes(StandardCharsets.UTF_8));
        headers.add("retry-error", errorMsg.getBytes(StandardCharsets.UTF_8));
        return headers;
    }
}