package com.csharma.adapterx.properties;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;


@Data
@ConfigurationProperties(prefix = "kafka.retry")
public class KafkaRetryProperties {

    private final Side producer = new Side();
    private final Side consumer = new Side();


    @Data
    @NoArgsConstructor
    public static class Side {
        private boolean enabled;
        private int maxAttempts;
        private long backoff;
        private String recoveryStrategy;
        private List<String> retryable = new ArrayList<>();
        private List<String> nonRetryable = new ArrayList<>();
    }
}
