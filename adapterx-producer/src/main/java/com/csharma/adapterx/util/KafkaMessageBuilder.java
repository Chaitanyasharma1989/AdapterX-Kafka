package com.csharma.adapterx.util;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;

public class KafkaMessageBuilder {

    private static final String MESSAGE_KEY =  "";
    private static final String PARTITION_ID = "";

    public static <T> Message<T> buildMessage(final String topic, final String key, final Integer partition,
                                              final T payload, final Map<String, String> customHeaders) {

        final MessageBuilder<T> builder = MessageBuilder
                .withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, topic);

        if (key != null) {
            builder.setHeader(MESSAGE_KEY, key);
        }

        if (partition != null) {
            builder.setHeader(PARTITION_ID, partition);
        }

        if (customHeaders != null) {
            customHeaders.forEach(builder::setHeader);
        }

        return builder.build();
    }
}
