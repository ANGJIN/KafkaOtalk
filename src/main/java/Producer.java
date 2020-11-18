import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Producer {

  public static RecordMetadata Produce(String topic, String key, String value)
      throws ExecutionException, InterruptedException {
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ProducerConfig.CLIENT_ID_CONFIG, "id1");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    config.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

    KafkaProducer<String, String> producer = new KafkaProducer<>(config);
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

    RecordMetadata sendResult = producer.send(record).get();
    producer.close();
    return sendResult;
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
  }
}
