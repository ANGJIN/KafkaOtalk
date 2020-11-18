import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class KafkaConnectionTest {
    public static KafkaConnection connection;

    public KafkaConnectionTest() {
        connection = new KafkaConnection();
    }

    public static String getTestTopicName() {
        Random rand = new Random();
        rand.setSeed(System.currentTimeMillis());
        String randNo = Integer.toString(rand.nextInt(10000));
        return "testTopic"+randNo;
    }

    @Test
    public void testConsume() throws ExecutionException, InterruptedException {
        String value = Double.toString(Math.random() * 100 % 100);
        String topic = getTestTopicName();

        if(connection.CheckTopic(topic)) {
            connection.DeleteTopic(topic);
        }
        connection.CreateTopic(topic);

        connection.setProducer("testProducer");
        connection.Produce(topic, "testKey", value);
        connection.setConsumer("testConsumer");
        var consumerRecords = connection.Consume(topic);

        assertThat(consumerRecords.iterator().next().value(), is(value));
        connection.DeleteTopic(topic);
    }

    @Test
    public void testProduce() throws ExecutionException, InterruptedException {
        String value = Double.toString(Math.random() * 100 % 100);
        String topic = getTestTopicName();
        if (connection.CheckTopic(topic)) {
            connection.DeleteTopic(topic);
        }
        connection.CreateTopic(topic);

        connection.setProducer("testProducer");
        RecordMetadata metadata = connection.Produce(topic, "testKey", value);

        assertThat(metadata.topic(), is(topic));
        assertThat(metadata.serializedKeySize(), is(7));
        assertThat(metadata.serializedValueSize(), is(value.length()));

        connection.DeleteTopic(topic);
    }

    @Test
    public void testGetAllTopics() throws ExecutionException, InterruptedException {
        Set<String> topics = connection.GetAllTopics();
        for (String topic : topics) {
            System.out.println(topic);
        }

        assertThat(topics, is(notNullValue()));
    }

    @Test
    public void testGetAllConsumerGroups() throws ExecutionException, InterruptedException {
        var groups = connection.GetAllConsumerGroups();
        for (var group : groups) {
            System.out.println(group.groupId());
        }

        assertThat(groups, is(notNullValue()));
    }

    @Test
    public void testCheckTopic() throws ExecutionException, InterruptedException {
        String topic = getTestTopicName();

        connection.setProducer("testProducer");
        connection.Produce(topic, "test", "test");
        boolean isTopicExist = connection.CheckTopic(topic);
        connection.DeleteTopic(topic);

        assertThat(isTopicExist, is(true));
    }

    @Test
    public void testCreateTopic() throws ExecutionException, InterruptedException {
        String topic = getTestTopicName();

        if (connection.CheckTopic(topic)) {
            connection.DeleteTopic(topic);
        }
        boolean success = connection.CreateTopic(topic);

        assertThat(success, is(true));

        connection.DeleteTopic(topic);
    }

    @Test
    public void testDeleteTopic() throws ExecutionException, InterruptedException {
        String topic = getTestTopicName();

        if (!connection.CheckTopic(topic)) {
            connection.CreateTopic(topic);
        }
        boolean success = connection.DeleteTopic(topic);

        assertThat(success, is(true));
    }

    @Test
    public void testSeekToBeginning() throws ExecutionException, InterruptedException {
        String topic = getTestTopicName();

        if (connection.CheckTopic(topic)) {
            connection.DeleteTopic(topic);
        }
        connection.CreateTopic(topic);

        connection.setConsumer("testConsumer");
        connection.setProducer("testProducer");

        connection.Produce(topic, "testKey", "testValue");
        var consumeResult = connection.Consume(topic);
        var first = consumeResult.iterator().next();

        connection.Produce(topic, "testKey2", "testValue2");
        connection.SeekToBeginning(topic);
        consumeResult = connection.Consume(topic);
        var second = consumeResult.iterator().next();

        assertThat(first.value(), is(second.value()));

        connection.DeleteTopic(topic);
    }
}
