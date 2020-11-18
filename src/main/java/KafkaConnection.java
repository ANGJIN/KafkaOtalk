import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaConnection {
    public KafkaConsumer<String, String> Consumer;
    public KafkaProducer<String, String> Producer;
    public static AdminClient Admin;

    public KafkaConnection() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        if (Admin == null) {
            Admin = AdminClient.create(config);
        }
    }

    public void setConsumer(String consumerId) {
        Properties config = new Properties();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerId);
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.Consumer = new KafkaConsumer<>(config);
    }

    public void setProducer(String producerId) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        this.Producer = new KafkaProducer<>(config);
    }

    public Iterable<ConsumerRecord<String, String>> Consume(String topic) {
        KafkaConsumer<String, String> consumer = this.Consumer;
        assert (consumer != null);

        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, String> records = null;
        try {
            do {
                records = consumer.poll(Duration.ofMillis(1000));
            } while (records.isEmpty());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        assert records != null;
        return records.records(topic);
    }

    public RecordMetadata Produce(String topic, String key, String value)
            throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = this.Producer;
        assert (producer != null);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        //producer.close();
        return producer.send(record).get();
    }

    public Set<String> GetAllTopics() throws ExecutionException, InterruptedException {
        return KafkaConnection.Admin.listTopics().names().get();
    }

    public Collection<ConsumerGroupListing> GetAllConsumerGroups() throws ExecutionException, InterruptedException {
        return KafkaConnection.Admin.listConsumerGroups().all().get();
    }

    public boolean CheckTopic(String topicName) throws ExecutionException, InterruptedException {
        return GetAllTopics().contains(topicName);
    }

    public boolean CreateTopic(String topicName) throws ExecutionException, InterruptedException {
        if (CheckTopic(topicName)) return true; // already exists

        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);

        var createResult = Admin.createTopics(Collections.singletonList(newTopic)).values().get(topicName);
        createResult.get();
        return createResult.isDone();
    }

    public boolean DeleteTopic(String topicName) throws ExecutionException, InterruptedException {
        if (!CheckTopic(topicName)) return false; // no topic

        var deleteResult = Admin.deleteTopics(Collections.singletonList(topicName)).values().get(topicName);
        deleteResult.get();
        return deleteResult.isDone();
    }

    public void SeekToBeginning(String topicName) throws ExecutionException, InterruptedException {
        assert (CheckTopic(topicName));
        assert (Consumer != null);
        TopicPartition partition = new TopicPartition(topicName,0);
        Consumer.seekToBeginning(Collections.singletonList(partition));
    }
}
