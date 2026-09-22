package com.marketdatapipeline.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
public class TickConsumer {

  private static final Logger log = LoggerFactory.getLogger(TickConsumer.class);
  private static final String TOPIC = "ticks";

  private final ObjectMapper objectMapper;

  public TickConsumer(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @KafkaListener(topics = TOPIC)
  public void onTick(ConsumerRecord<String, String> record) {
    JsonNode value = objectMapper.readTree(record.value());
    String price = value.path("price").asString(null);

    log.atInfo()
        .addKeyValue("symbol", record.key())
        .addKeyValue("partition", record.partition())
        .addKeyValue("offset", record.offset())
        .addKeyValue("price", price)
        .log("tick received");
  }
}
