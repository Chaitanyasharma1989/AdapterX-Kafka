package com.csharma.adapterX.producer.exception;

public class KafkaProducerException extends RuntimeException {

  public KafkaProducerException(String message) {
    super(message);
  }
  public KafkaProducerException(String message, Throwable throwable) {
    super(message,throwable);
  }
}
