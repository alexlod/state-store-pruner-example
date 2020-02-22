package io.confluent.ps.examples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MyProducer {
  private static final Logger log = LoggerFactory.getLogger(MyProducer.class);

  public static final int MESSAGE_INTERVAL_MS = 60000;

  public static final String INPUT_TOPIC = "test";

  private static Properties createConfig() {
    Properties props = new Properties();

    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class);

    return props;
  }

  public static void main(String[] args) {
    log.info("Starting...");

    Producer<String, String> producer = new KafkaProducer<>(createConfig());

    long count = 0;
    while (true) {
      ProducerRecord<String, String> record = new ProducerRecord<>(INPUT_TOPIC, "key", "value-" + count);
      producer.send(record, (metadata, error) -> {
        if (error == null) {
          log.info("Produced a message with offset " + metadata.offset());
        } else {
          log.error("An error occurred producing a message.");
        }
      });
      try {
        Thread.sleep(MESSAGE_INTERVAL_MS);
      } catch (InterruptedException e) {
      }
      count++;
    }
  }
}
