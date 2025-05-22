package com.csharma.adapterx.retry;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;


@Repository
public interface FailedEventRepository extends JpaRepository<KafkaRetryEvent, Long> {
    List<KafkaRetryEvent> findTop100ByStatusOrderByCreatedAtAsc(String status);
}
