package com.csharma.adapterX.producer;

public interface KafkaRecoveryStrategy {

    void recover(String payload, KafkaRecoveryContext kafkaRecoveryContext, Exception ex);
}
