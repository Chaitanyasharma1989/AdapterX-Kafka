package com.csharma.adapterX.producer;

import lombok.*;

import java.util.Objects;


@Data
@AllArgsConstructor
@Builder
public final class KafkaRecoveryContext {
    private final String flowType;
    private final String topic;
    private final Integer partition;
    private final Long offset;
    private final String key;
}