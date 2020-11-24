import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class KafkaConnection {
    public KafkaConsumer<String, String> Consumer;
    public KafkaProducer<String, String> Producer;
    public static AdminClient Admin;
    private String KafkaServerAddress = "localhost:9092";

    public void setKafkaServerAddress(String address) {
        KafkaServerAddress = address;
    }


    public KafkaConnection() {
        setKafkaServerAddress("localhost:9092");

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServerAddress);
        if (Admin == null) {
            Admin = AdminClient.create(config);
        }
    }

    public KafkaConnection(String serverAddress) {
        setKafkaServerAddress(serverAddress);

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServerAddress);
        if (Admin == null) {
            Admin = AdminClient.create(config);
        }
    }

    public void setConsumer(String consumerId) {
        Properties config = new Properties();
        config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerId);
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServerAddress);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");

        this.Consumer = new KafkaConsumer<>(config);

        RestoreOffsetInfo();
    }

    public void setProducer(String producerId) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaServerAddress);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        //config.put(ProducerConfig.LINGER_MS_CONFIG, 1);
//        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        this.Producer = new KafkaProducer<>(config);
    }

    public void SaveOffsetInfo() {
        HashMap<String, Long> savedOffset = new HashMap<>();

        for (String subTopic : GetSubscribedTopics()) {
            if(subTopic.equals("ConsumerInfo"))
                continue;

            int partition = Consumer.partitionsFor(subTopic).get(0).partition();
            var offset = Consumer.position(new TopicPartition(subTopic, partition));

            savedOffset.put(subTopic, offset);
        }
        String consumerId = Consumer.groupMetadata().groupId();
        Produce("ConsumerInfo", consumerId, savedOffset.toString());

    }

//    public void SubscribeAndSeek(String topic, long offset) {
//        KafkaConsumer<String, String> consumer = this.Consumer;
//        assert (consumer != null);
//
//        var subscription = GetSubscribedTopics();
//
//        if (subscription.isEmpty()) {
//            consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
//                @Override
//                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//
//                }
//
//                @Override
//                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//                    Seek(topic,offset);
//                }
//            });
//
//        } else if (!subscription.contains(topic)) {
//            HashSet<String> newSub = new HashSet<>(subscription);
//            newSub.add(topic);
//
//            consumer.subscribe(newSub, new ConsumerRebalanceListener() {
//                @Override
//                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//
//                }
//
//                @Override
//                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//                    Seek(topic,offset);
//                }
//            });
//        }
//    }

    public void RestoreOffsetInfo() {
//        HashMap<String, Long> restoredOffset = new HashMap<>();
        String consumerId = Consumer.groupMetadata().groupId();

        if (GetSubscribedTopics().contains("ConsumerInfo"))
            SeekToBeginning("ConsumerInfo");
//        SubscribeTopic("ConsumerInfo");

        var InfoRecords = StreamSupport.stream(Consume("ConsumerInfo").spliterator(),false)
                .collect(Collectors.toList());
        Consumer.unsubscribe();
        //SeekToBeginning("ConsumerInfo");
        for (int i=InfoRecords.size()-1; i>0; i--) {
            var record = InfoRecords.get(i);
            if (record.key().equals(consumerId)) {
                String offsetInfo = record.value();
                offsetInfo = offsetInfo.substring(1, offsetInfo.length() - 1).replace(" ", "");

                if (offsetInfo.isEmpty())
                    break;

                for (String info : offsetInfo.split(",")) {
                    String topic = info.split("=")[0];
                    long offset = Long.parseLong(info.split("=")[1]);
                    SubscribeTopic(topic);
                    Seek(topic,offset);
//                    SubscribeAndSeek(topic,offset);
//                    restoredOffset.put(topic, offset);
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            }
        }

    }

    public Iterable<ConsumerRecord<String, String>> Consume(String topic) {
        KafkaConsumer<String, String> consumer = this.Consumer;
        assert (consumer != null);
        SubscribeTopic(topic);

        HashMap<String, Long> savedOffset = new HashMap<>();

        for (String subTopic : GetSubscribedTopics()) {
            if (!subTopic.equals(topic)) {
                int partition = Consumer.partitionsFor(subTopic).get(0).partition();
                var offset = Consumer.position(new TopicPartition(subTopic, partition));
                savedOffset.put(subTopic, offset);
            }
        }

        var time = new Date().getTime();

        ConsumerRecords<String, String> records = null;
        try {
            do {
                records = consumer.poll(Duration.ofMillis(1000));
            } while (records.count() == 0 && new Date().getTime() - time < 1000);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        savedOffset.forEach(this::Seek);

        assert records != null;
        return records.records(topic);
    }

    public RecordMetadata Produce(String topic, String key, String value) {
        KafkaProducer<String, String> producer = this.Producer;
        assert (producer != null);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        final RecordMetadata[] sendResult = {null};
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    sendResult[0] = metadata;
                }
            });
        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            producer.flush();
        }

        try {
            while (sendResult[0] == null) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return sendResult[0];
    }

    public Set<String> GetAllTopics() {
        Set<String> result = null;
        try {
            result = KafkaConnection.Admin.listTopics().names().get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        assert result != null;
        return result;
    }

    public Collection<ConsumerGroupListing> GetAllConsumerGroups() throws ExecutionException, InterruptedException {
        return KafkaConnection.Admin.listConsumerGroups().all().get();
    }

    public boolean CheckTopic(String topicName) {
        return GetAllTopics().contains(topicName);
    }

    public boolean CreateTopic(String topicName) {
        if (CheckTopic(topicName)) return true; // already exists

        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
        KafkaFuture<Void> createResult = null;
        boolean result = false;
        try {
            createResult = Admin.createTopics(Collections.singletonList(newTopic)).values().get(topicName);
            createResult.get();
            result = createResult.isDone();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public boolean DeleteTopic(String topicName) {
        if (!CheckTopic(topicName)) return false; // no topic
        KafkaFuture<Void> deleteResult = null;
        try {
            deleteResult = Admin.deleteTopics(Collections.singletonList(topicName)).values().get(topicName);
            deleteResult.get();
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assert deleteResult != null;
        return deleteResult.isDone();
    }

    public void Seek(String topic, long offset) {
        int partition = Consumer.partitionsFor(topic).get(0).partition();
        Consumer.seek(new TopicPartition(topic, partition), offset);
    }
    public void SeekToBeginning(String topicName) {
        assert (CheckTopic(topicName));
        assert (Consumer != null);

        if (!GetSubscribedTopics().contains(topicName)) {
            SubscribeTopic(topicName);
        }

        int partitionNumber = Consumer.partitionsFor(topicName).get(0).partition();
        TopicPartition partition = new TopicPartition(topicName, partitionNumber);
        try {
            Consumer.seekToBeginning(Collections.singletonList(partition));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Set<String> GetSubscribedTopics() {
        assert (Consumer != null);

        //return Consumer.subscription();
        HashSet<String> topics = new HashSet<>();
        for (var info : Consumer.assignment()) {
            topics.add(info.topic());
        }
        return topics;
    }

    public void ConsumePause(String topic) {
        assert Consumer != null;
        if (GetSubscribedTopics().contains(topic)) {
            int partition = Consumer.partitionsFor(topic).get(0).partition();
            Consumer.pause(Collections.singletonList(new TopicPartition(topic, partition)));
        }
    }

    public void ConsumeResume(String topic) {
        assert Consumer != null;
        int partition = Consumer.partitionsFor(topic).get(0).partition();
        Consumer.resume(Collections.singletonList(new TopicPartition(topic, partition)));
    }

    public void SubscribeTopic(String topic) {
        KafkaConsumer<String, String> consumer = this.Consumer;
        assert (consumer != null);

        var subscription = GetSubscribedTopics();

        if (subscription.isEmpty()) {
//            consumer.subscribe(Collections.singletonList(topic));
            consumer.assign(Collections.singletonList(new TopicPartition(topic,0)));

        } else if (!subscription.contains(topic)) {
//            HashSet<String> newSub = new HashSet<>(subscription);
//            newSub.add(topic);
//            consumer.subscribe(newSub);
            HashSet<TopicPartition> newPartition = new HashSet<>();
            for(String oldTopic : subscription) {
                newPartition.add(new TopicPartition(oldTopic,0));
            }
            newPartition.add(new TopicPartition(topic,0));

            consumer.assign(newPartition);
        }
    }

//    public void DeleteRecord(String topic, long offset) {
//        int partitionNum = Consumer.partitionsFor(topic).get(0).partition();
//        TopicPartition partition = new TopicPartition(topic, partitionNum);
//        Admin.deleteRecords(Collections.singletonMap(partition, RecordsToDelete(offset)));
//    }

}
