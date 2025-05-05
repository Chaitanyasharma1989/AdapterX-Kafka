# AdapterX-Kafka
Kafka consumer and producer implementation of AdapterX using Spring-Kafka 


Use RetryTemplate inside Kafka (ConcurrentKafkaListenerContainerFactory) with a RecoveryCallback or DeadLetterPublishingRecoverer.

This ensures:
•	Correct Kafka offset control (no duplicates or message loss).
•	Seamless DLQ integration.
•	Works with batch processing, manual acks, and advanced error handling.

When to use Spring Retry (@Retryable)?

Only use @Retryable if:
•	You’re not consuming Kafka directly (e.g., just retrying service logic).
•	You’re retrying downstream service calls, like HTTP or DB ops, outside of message consumption.

@Retryable (Outside Kafka)         RetryTemplate / ErrorHandler (Inside Kafka)

Kafka Container                    Kafka Container
↓                                  ↓
Listener call intercepted          Listener method
by Spring Retry                    wrapped in RetryTemplate
↓                                  ↓
Retry x3                           Retry x3
↓                                  ↓
Kafka sees 1 failure              Kafka sees 3 attempts
(message may be redelivered)      (DLT or commit managed correctly)


1. Consumer Offset Management:

Kafka consumers track the offset of messages they have processed. The offset indicates the position in a topic partition that the consumer has read up to. By default, Kafka consumers commit the offset only after successfully processing a message.
•	Automatic Offset Commit (enable.auto.commit = true): When enabled, Kafka automatically commits the offset of the last successfully consumed message after each fetch cycle.
•	Manual Offset Commit (enable.auto.commit = false): Consumers have full control over when to commit the offset, usually after successfully processing a message.

⸻

2. Retry Scenarios and Offset Handling:

When the consumer fails to process a message after a retry (e.g., retries exceed the limit or after a failure), the offset is not committed until the message is successfully processed. Here’s what happens based on your configuration:

Retry with RetryTemplate or @Retryable:
•	In-memory retry: If you use an in-memory retry mechanism (like RetryTemplate), the consumer has not committed the offset yet. Once retries are exhausted, the consumer can either commit the last successfully processed message or let the offset remain uncommitted.
•	Offsets and Retries: Kafka doesn’t automatically handle retries; it depends on how you configure the consumer to retry. After the retries, if the message is not successfully consumed, you must decide what to do with the offset.

Dead Letter Queue (DLQ):

If after retries the event still cannot be processed, you can send it to a Dead Letter Queue (DLQ). When you send to a DLQ, the main consumer will not commit the offset of the original message until it is processed. This means that:
•	If you are manually managing offsets, you can ensure that the consumer does not skip any messages.
•	If Kafka automatically commits offsets (with enable.auto.commit = true), you would need to handle re-processing and message duplication, as Kafka may commit the offset before retry attempts are exhausted.

⸻

3. Handling Offset Commit Failure:

If the consumer doesn’t successfully process a message, you can:
•	Not commit the offset: This will cause Kafka to re-deliver the same message the next time the consumer polls for messages.
•	Move the offset to DLQ: If you want to ensure that the consumer can continue with the next message in line, you could either:
•	Commit the offset manually if you’re done retrying.
•	Send the failed message to a Dead Letter Topic (DLT) and commit the offset if retries have failed.


4. Key Scenarios for Offset Management After Retries:
   •	Retries Exhausted, Send to DLQ: If after retries the message is not processed, you would commit the offset after sending it to a DLQ. This ensures the consumer moves past the message and doesn’t block further consumption.
   •	Message Processed Partially: If a message is partially processed (e.g., you process some side effects, but an error prevents completion), you may not commit the offset, ensuring that Kafka will attempt to re-deliver the message.

⸻

5. Dead Letter Queue (DLQ):
   •	Per-Topic DLQs: In production systems, it’s common to send the message to a per-topic DLQ (e.g., orders.DLT) when retries exceed the limit.
   •	Automatic Offset Management: After the message is sent to the DLQ, the offset for that message should be committed manually to allow the consumer to process the next message in the queue.

⸻

Summary of Kafka Offset Management for Retries:
1.	Retry Logic: Kafka will not commit the offset until the message is successfully processed. If retries are exhausted, the message can either be:
    •	Sent to a Dead Letter Queue (DLQ).
    •	The offset can be committed manually to continue consuming other messages.
2.	Offset Handling:
    •	If retries are unsuccessful, you can commit the offset manually to avoid reprocessing.
    •	Use enable.auto.commit = false to have control over when the offset is committed, especially if you’re handling retries and DLQ logic.
3.	DLQ & Offset Commit: If you decide to use a DLQ, commit the offset after sending the message to the DLQ to ensure the consumer can continue consuming the next available messages.