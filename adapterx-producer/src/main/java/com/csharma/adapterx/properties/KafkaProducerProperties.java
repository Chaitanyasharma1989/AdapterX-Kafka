package com.csharma.adapterx.properties;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "kafka.producer")
public class KafkaProducerProperties {

    private String bootstrapServers;
    private String acks = "all";
    private int retries = 3;
    private int batchSize = 16384;
    private int lingerMs = 1;
    private long bufferMemory = 33554432;
    private int bulkSize = 100;

    private RetryProperties retry = new RetryProperties();

    @Data
    public static class RetryProperties {
        private int maxAttempts = 3;
        private long initialInterval = 1000L;
        private double multiplier = 2.0;
        private long maxInterval = 10000L;
    }
}
