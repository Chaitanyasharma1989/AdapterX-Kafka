package com.csharma.adapterx.delegate;


import com.csharma.adapterx.model.BulkProducerRequest;
import com.csharma.adapterx.model.ProducerRequest;
import com.csharma.adapterx.model.ProducerResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class KafkaProducerDelegate {

    private final KafkaTemplate<String, Object> standardTemplate;
    private final KafkaTemplate<String, Object> transactionalTemplate;
    private final KafkaTemplate<String, Object> bulkTemplate;
    private final ReplyingKafkaTemplate<String, Object, Object> replyingTemplate;
    private final KafkaTransactionManager transactionManager;

    public KafkaProducerDelegate(
            @Qualifier("kafkaTemplate") KafkaTemplate<String, Object> standardTemplate,
            @Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, Object> transactionalTemplate,
            @Qualifier("bulkKafkaTemplate") KafkaTemplate<String, Object> bulkTemplate,
            ReplyingKafkaTemplate<String, Object, Object> replyingTemplate,
            KafkaTransactionManager transactionManager) {
        this.standardTemplate = standardTemplate;
        this.transactionalTemplate = transactionalTemplate;
        this.bulkTemplate = bulkTemplate;
        this.replyingTemplate = replyingTemplate;
        this.transactionManager = transactionManager;
    }

    // Main delegation method for single messages
    public <K, V> CompletableFuture<ProducerResponse<SendResult<K, V>>> publish(ProducerRequest<K, V> request) {
        long startTime = System.currentTimeMillis();

        return switch (request.type()) {
            case STANDARD -> publishStandard(request, startTime);
            case TRANSACTIONAL -> publishTransactional(request, startTime);
            case BULK -> throw new IllegalArgumentException("Use publishBulk method for bulk operations");
            case REPLY -> publishWithReply(request, startTime);
        };
    }

    // Bulk publishing method
    public <K, V> CompletableFuture<ProducerResponse<List<SendResult<K, V>>>> publishBulk(BulkProducerRequest<K, V> request) {
        long startTime = System.currentTimeMillis();

        if (request.enableTransaction()) {
            return publishBulkTransactional(request, startTime);
        } else {
            return publishBulkStandard(request, startTime);
        }
    }

    // Reply-enabled publishing
    public <K, V, R> CompletableFuture<ProducerResponse<R>> publishAndWaitForReply(
            ProducerRequest<K, V> request, Class<R> replyType) {
        long startTime = System.currentTimeMillis();

        try {
            ProducerRecord<String, Object> record = createProducerRecord(request);

            CompletableFuture<ConsumerRecord<String, Object>> replyFuture =
                    replyingTemplate.sendAndReceive(record, request.timeout());

            return replyFuture.handle((reply, throwable) -> {
                long processingTime = System.currentTimeMillis() - startTime;

                if (throwable != null) {
                    log.error("Failed to receive reply for topic: {}", request.topic(), throwable);
                    return ProducerResponse.<R>failure(new RuntimeException(throwable), processingTime);
                }

                try {
                    R result = convertReply(reply.value(), replyType);
                    log.debug("Received reply for topic: {} in {}ms", request.topic(), processingTime);
                    return ProducerResponse.success(result, null, processingTime);
                } catch (Exception e) {
                    log.error("Failed to convert reply for topic: {}", request.topic(), e);
                    return ProducerResponse.<R>failure(e, processingTime);
                }
            });

        } catch (Exception e) {
            long processingTime = System.currentTimeMillis() - startTime;
            log.error("Failed to send message with reply for topic: {}", request.topic(), e);
            return CompletableFuture.completedFuture(
                    ProducerResponse.<R>failure(e, processingTime));
        }
    }

    // Standard publishing
    @SuppressWarnings("unchecked")
    private <K, V> CompletableFuture<ProducerResponse<SendResult<K, V>>> publishStandard(
            ProducerRequest<K, V> request, long startTime) {
        try {
            ProducerRecord<String, Object> record = createProducerRecord(request);

            CompletableFuture<SendResult<String, Object>> future = standardTemplate.send(record);

            return future.handle((result, throwable) -> {
                long processingTime = System.currentTimeMillis() - startTime;

                if (throwable != null) {
                    log.error("Failed to send message to topic: {}", request.topic(), throwable);
                    return ProducerResponse.<SendResult<K, V>>failure(new RuntimeException(throwable), processingTime);
                }

                log.debug("Successfully sent message to topic: {} in {}ms", request.topic(), processingTime);
                return ProducerResponse.success((SendResult<K, V>) result, result.getRecordMetadata(), processingTime);
            });

        } catch (Exception e) {
            long processingTime = System.currentTimeMillis() - startTime;
            log.error("Failed to send message to topic: {}", request.topic(), e);
            return CompletableFuture.completedFuture(
                    ProducerResponse.<SendResult<K, V>>failure(e, processingTime));
        }
    }

    // Transactional publishing
    @SuppressWarnings("unchecked")
    private <K, V> CompletableFuture<ProducerResponse<SendResult<K, V>>> publishTransactional(
            ProducerRequest<K, V> request, long startTime) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return transactionalTemplate.executeInTransaction(template -> {
                    try {
                        ProducerRecord<String, Object> record = createProducerRecord(request);
                        SendResult<String, Object> result = template.send(record).get(request.timeout().toMillis(), TimeUnit.MILLISECONDS);

                        long processingTime = System.currentTimeMillis() - startTime;
                        log.debug("Successfully sent transactional message to topic: {} in {}ms", request.topic(), processingTime);
                        return ProducerResponse.success((SendResult<K, V>) result, result.getRecordMetadata(), processingTime);

                    } catch (Exception e) {
                        long processingTime = System.currentTimeMillis() - startTime;
                        log.error("Failed to send transactional message to topic: {}", request.topic(), e);
                        throw new RuntimeException(e);
                    }
                });
            } catch (Exception e) {
                long processingTime = System.currentTimeMillis() - startTime;
                return ProducerResponse.<SendResult<K, V>>failure(e, processingTime);
            }
        });
    }

    // Reply publishing
    @SuppressWarnings("unchecked")
    private <K, V> CompletableFuture<ProducerResponse<SendResult<K, V>>> publishWithReply(
            ProducerRequest<K, V> request, long startTime) {
        try {
            ProducerRecord<String, Object> record = createProducerRecord(request);

            // Set reply topic header if not present
            if (!record.headers().iterator().hasNext() ||
                    !hasHeader(record, KafkaHeaders.REPLY_TOPIC)) {
                record.headers().add(KafkaHeaders.REPLY_TOPIC, (request.topic() + ".reply").getBytes());
            }

            CompletableFuture<SendResult<String, Object>> future = standardTemplate.send(record);

            return future.handle((result, throwable) -> {
                long processingTime = System.currentTimeMillis() - startTime;

                if (throwable != null) {
                    log.error("Failed to send reply-enabled message to topic: {}", request.topic(), throwable);
                    return ProducerResponse.<SendResult<K, V>>failure(new RuntimeException(throwable), processingTime);
                }

                log.debug("Successfully sent reply-enabled message to topic: {} in {}ms", request.topic(), processingTime);
                return ProducerResponse.success((SendResult<K, V>) result, result.getRecordMetadata(), processingTime);
            });

        } catch (Exception e) {
            long processingTime = System.currentTimeMillis() - startTime;
            log.error("Failed to send reply-enabled message to topic: {}", request.topic(), e);
            return CompletableFuture.completedFuture(
                    ProducerResponse.<SendResult<K, V>>failure(e, processingTime));
        }
    }

    // Bulk standard publishing
    @SuppressWarnings("unchecked")
    private <K, V> CompletableFuture<ProducerResponse<List<SendResult<K, V>>>> publishBulkStandard(
            BulkProducerRequest<K, V> request, long startTime) {
        try {
            List<CompletableFuture<SendResult<String, Object>>> futures = request.records().stream()
                    .map(record -> bulkTemplate.send((ProducerRecord<String, Object>) record))
                    .toList();

            CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

            return allOf.handle((v, throwable) -> {
                long processingTime = System.currentTimeMillis() - startTime;

                if (throwable != null) {
                    log.error("Failed to send bulk messages", throwable);
                    return ProducerResponse.<List<SendResult<K, V>>>failure(new RuntimeException(throwable), processingTime);
                }

                List<SendResult<K, V>> results = futures.stream()
                        .map(future -> {
                            try {
                                return (SendResult<K, V>) future.get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .toList();

                log.debug("Successfully sent {} bulk messages in {}ms", results.size(), processingTime);
                return ProducerResponse.success(results, null, processingTime);
            });

        } catch (Exception e) {
            long processingTime = System.currentTimeMillis() - startTime;
            log.error("Failed to send bulk messages", e);
            return CompletableFuture.completedFuture(
                    ProducerResponse.<List<SendResult<K, V>>>failure(e, processingTime));
        }
    }

    // Bulk transactional publishing
    @SuppressWarnings("unchecked")
    private <K, V> CompletableFuture<ProducerResponse<List<SendResult<K, V>>>> publishBulkTransactional(
            BulkProducerRequest<K, V> request, long startTime) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return transactionalTemplate.executeInTransaction(template -> {
                    try {
                        List<SendResult<K, V>> results = new ArrayList<>();

                        for (ProducerRecord<K, V> record : request.records()) {
                            SendResult<String, Object> result = template.send((ProducerRecord<String, Object>) record)
                                    .get(request.timeout().toMillis(), TimeUnit.MILLISECONDS);
                            results.add((SendResult<K, V>) result);
                        }

                        long processingTime = System.currentTimeMillis() - startTime;
                        log.debug("Successfully sent {} transactional bulk messages in {}ms", results.size(), processingTime);
                        return ProducerResponse.success(results, null, processingTime);

                    } catch (Exception e) {
                        long processingTime = System.currentTimeMillis() - startTime;
                        log.error("Failed to send transactional bulk messages", e);
                        throw new RuntimeException(e);
                    }
                });
            } catch (Exception e) {
                long processingTime = System.currentTimeMillis() - startTime;
                return ProducerResponse.<List<SendResult<K, V>>>failure(e, processingTime);
            }
        });
    }

    // Helper methods
    private <K, V> ProducerRecord<String, Object> createProducerRecord(ProducerRequest<K, V> request) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(
                request.topic(),
                (String) request.key(),
                request.value()
        );

        // Add headers
        request.headers().forEach((key, value) -> {
            if (value instanceof String) {
                record.headers().add(key, ((String) value).getBytes());
            } else if (value instanceof byte[]) {
                record.headers().add(key, (byte[]) value);
            }
        });

        return record;
    }

    private boolean hasHeader(ProducerRecord<String, Object> record, String headerName) {
        for (Header header : record.headers()) {
            if (header.key().equals(headerName)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private <R> R convertReply(Object reply, Class<R> replyType) {
        if (replyType.isInstance(reply)) {
            return (R) reply;
        }

        // Add custom conversion logic here if needed
        throw new IllegalArgumentException("Cannot convert reply to " + replyType.getSimpleName());
    }
}