package com.csharma.adapterX.producer;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "kafka_retry_events")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KafkaRetryEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String topic;
    private Integer partition;
    private Long offset;
    private String key;

    @Lob
    private String payload;

    private String exceptionMessage;
    private Integer retryCount;
    private String status;

    private LocalDateTime createdAt;
    private LocalDateTime lastAttemptAt;

}
