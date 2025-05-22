package com.csharma.adapterx.recovery;

public interface KafkaRecoveryStrategy {

    void recover(String payload, KafkaRecoveryContext kafkaRecoveryContext, Exception ex);
}
