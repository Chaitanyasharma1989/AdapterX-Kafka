package com.csharma.adapterX.producer;


import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;


@Slf4j
@Component("DLQ")
public class DlqRecoveryStrategy implements KafkaRecoveryStrategy {

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public DlqRecoveryStrategy(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void recover(String payload, KafkaRecoveryContext kafkaRecoveryContext, Exception ex) {
        String dlqTopic = kafkaRecoveryContext.getTopic() + ".dlq";
        log.warn("[{} Failure] topic={}, partition={}, offset={}, error={}",
                kafkaRecoveryContext.getFlowType(),
                kafkaRecoveryContext.getTopic(),
                kafkaRecoveryContext.getPartition(),
                kafkaRecoveryContext.getOffset(),
                ex.getMessage());

        // Optionally publish to DLQ
        kafkaTemplate.send(dlqTopic, kafkaRecoveryContext.getKey(), payload);
    }
}
