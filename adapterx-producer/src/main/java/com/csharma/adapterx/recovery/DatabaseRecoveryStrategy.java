package com.csharma.adapterx.recovery;


import com.csharma.adapterx.retry.FailedEventRepository;
import com.csharma.adapterx.retry.KafkaRetryEvent;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component("DATABASE")
public class DatabaseRecoveryStrategy implements KafkaRecoveryStrategy {

    private final FailedEventRepository repository;

    public DatabaseRecoveryStrategy(FailedEventRepository repository) {
        this.repository = repository;
    }

    @Override
    public void recover(String payload, KafkaRecoveryContext kafkaRecoveryContext, Exception ex) {
        KafkaRetryEvent event = KafkaRetryEvent.builder()
                .topic(kafkaRecoveryContext.getTopic())
                .partition(kafkaRecoveryContext.getPartition())
                .offset(kafkaRecoveryContext.getOffset())
                .key(kafkaRecoveryContext.getKey())
                .payload(payload)
                .exceptionMessage(ex.getMessage())
                .retryCount(0)
                .status("PENDING")
                .createdAt(LocalDateTime.now())
                .lastAttemptAt(LocalDateTime.now())
                .build();

        repository.save(event);
    }
}
