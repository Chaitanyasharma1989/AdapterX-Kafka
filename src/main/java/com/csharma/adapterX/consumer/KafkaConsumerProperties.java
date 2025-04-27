package com.csharma.adapterX.consumer;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "kafka.consumer")
public class KafkaConsumerProperties {

    private String bootstrapServers;
    private String groupId;
    private String autoOffsetReset = "earliest";
    private boolean enableAutoCommit = false;
    private int maxPollRecords = 500;
    private int sessionTimeoutMs = 30000;
    private int heartbeatIntervalMs = 10000;
    private int concurrency = 3;
    private boolean batchListener = false;
    private long pollTimeoutMs = 5000;

    private RetryProperties retry = new RetryProperties();

    @Data
    public static class RetryProperties {
        private boolean enabled = true;
        private int maxAttempts = 3;
        private long initialInterval = 1000L;
        private double multiplier = 2.0;
        private long maxInterval = 10000L;
    }

}
