import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {

  public static ConsumerRecords<String, String> Consume(String topic) {
    Properties config = new Properties();
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "fo2s234dfsdf");
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
    consumer.subscribe(Collections.singletonList(topic));
    ConsumerRecords<String, String> records = null;
    try {
      while (records == null) {
        records = consumer.poll(Duration.ofMillis(1000));
/*
                for (ConsumerRecord<String, String> record : records)
                {
                    System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
*/
      }
    } finally {
      consumer.close();
    }
    return records;
  }

  public static void main(String[] args) {
    Consumer.Consume("testTopic");
  }
}
