package com.csharma.adapterX.producer;


import com.csharma.adapterX.producer.properties.KafkaProducerProperties;
import com.csharma.adapterX.producer.template.KafkaProducerTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;


@Service
@Slf4j
public class BulkPublisher<T> {

    @Autowired
    private KafkaProducerTemplate<T> producerTemplate;

    @Autowired
    private KafkaProducerProperties properties;

    public List<CompletableFuture<SendResult<String, T>>> publishBulk(String topic, List<T> messages) {
        log.info("Publishing bulk of {} Messages to topic {}", messages.size(), topic);

        List<CompletableFuture<SendResult<String, T>>> futures = new ArrayList<>();
        List<List<T>> batches = partition(messages, properties.getBulkSize());

        for (List<T> batch : batches) {
            for (T message : batch) {
                futures.add(producerTemplate.send(topic, message));
            }
        }
        return futures;
    }

    public List<Future<SendResult<String, T>>> publishBulkWithKeys(String topic, Map<String, T> keyedMessages) {
        log.info("Publishing bulk of {} keyed messages to topic {}", keyedMessages.size(), topic);

        List<Future<SendResult<String, T>>> futures = new ArrayList<>();
        List<Map.Entry<String, T>> entries = new ArrayList<>(keyedMessages.entrySet());
        List<List<Map.Entry<String, T>>> batches = partition(entries, properties.getBulkSize());

        for (List<Map.Entry<String, T>> batch : batches) {
            for (Map.Entry<String, T> entry : batch) {
                futures.add(producerTemplate.send(topic, entry.getKey(), entry.getValue()));
            }
        }
        return futures;
    }

    private <E> List<List<E>> partition(List<E> list, int size) {
        List<List<E>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }
}