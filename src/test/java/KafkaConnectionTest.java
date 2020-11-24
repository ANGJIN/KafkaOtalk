import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class KafkaConnectionTest {
    public static KafkaConnection connection;

    public KafkaConnectionTest() {
        connection = new KafkaConnection("localhost:9092");
    }

    public static String getTestTopicName() {

        return "testTopic" + getRandIntString();
    }

    public static String getTestConsumerName() {
        return "testConsumer" + getRandIntString();
    }

    public static String getTestProducerName() {
        return "testProducer" + getRandIntString();
    }

    public static String getRandIntString() {
        Random rand = new Random();
        rand.setSeed(System.currentTimeMillis());
        return Integer.toString(rand.nextInt(10000));
    }

    @Test
    public void testConsume() throws ExecutionException, InterruptedException {
        String value = Double.toString(Math.random() * 100 % 100);
        String topic = getTestTopicName();

        if (connection.CheckTopic(topic)) {
            connection.DeleteTopic(topic);
        }
        connection.CreateTopic(topic);

        connection.setProducer(getTestProducerName());
        connection.Produce(topic, "testKey", value);
        connection.setConsumer(getTestConsumerName());
        var consumerRecord = connection.Consume(topic).iterator().next();

        assertThat(consumerRecord.value(), is(value));
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

        connection.setProducer(getTestProducerName());
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

        connection.setProducer(getTestProducerName());
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
        String topic2;
        do {
            topic2 = getTestTopicName();
        } while (topic.equals(topic2));

        if (connection.CheckTopic(topic)) {
            connection.DeleteTopic(topic);
        }
        connection.CreateTopic(topic);
        if(connection.CheckTopic(topic2)) {
            connection.DeleteTopic(topic2);
        }
        connection.CreateTopic(topic2);

        connection.setConsumer(getTestConsumerName());
        connection.setProducer(getTestProducerName());

        connection.Produce(topic, "testKey", "testValue");
        connection.Consume(topic);
        connection.Produce(topic, "testKey2", "testValue2");
        connection.Produce(topic2, "testKey3", "testValue3");
        connection.Produce(topic2, "testKey4","testValue4");
        connection.Consume(topic2);

        connection.SeekToBeginning(topic2);

        var first = connection.Consume(topic).iterator().next();

        var second = connection.Consume(topic2).iterator().next();

        assertThat(first.value(), is("testValue2"));
        assertThat(second.value(), is("testValue3"));

        connection.DeleteTopic(topic);
        connection.DeleteTopic(topic2);
    }

    @Test
    public void TestGetSubscribedTopics() throws ExecutionException, InterruptedException {
        String topic = getTestTopicName();
        String topic2;
        do {
            topic2 = getTestTopicName();
        } while (topic.equals(topic2));

        if (connection.CheckTopic(topic)) {
            connection.DeleteTopic(topic);
        }
        connection.CreateTopic(topic);

        if (connection.CheckTopic(topic2)) {
            connection.DeleteTopic(topic2);
        }
        connection.CreateTopic(topic2);

        connection.setConsumer(getTestConsumerName());
        connection.setProducer(getTestProducerName());

        connection.Produce(topic, "testKey", "testValue");
        connection.Produce(topic2, "testKey2", "testValue2");

        connection.Consume(topic);
        connection.Consume(topic2);

        var subscribeTopic = connection.GetSubscribedTopics();

        assertThat(subscribeTopic, hasItems(topic,topic2));

        connection.DeleteTopic(topic);
        connection.DeleteTopic(topic2);
    }

    @Test
    public void TestPauseResume() throws ExecutionException, InterruptedException {
        String topic = getTestTopicName();
        if(connection.CheckTopic(topic)) {
            connection.DeleteTopic(topic);
        }
        connection.CreateTopic(topic);
        connection.setProducer(getTestProducerName());
        connection.setConsumer(getTestConsumerName());

        connection.Consume(topic);
        connection.Produce(topic,"testKey","testValue");
        connection.ConsumePause(topic);
        var consumeResult = connection.Consume(topic);
        assertThat(consumeResult.iterator().hasNext(), is(false));

        connection.ConsumeResume(topic);
        consumeResult = connection.Consume(topic);
        assertThat(consumeResult.iterator().next().value(), is("testValue"));

        connection.DeleteTopic(topic);
    }

    @Test
    public void TestSaveAndRestoreOffset() throws InterruptedException {
        String topic = getTestTopicName();
        String topic2;
        do {
            topic2 = getTestTopicName();
        } while (topic.equals(topic2));

        if (connection.CheckTopic(topic)) {
            connection.DeleteTopic(topic);
        }
        connection.CreateTopic(topic);

        if (connection.CheckTopic(topic2)) {
            connection.DeleteTopic(topic2);
        }
        connection.CreateTopic(topic2);

        String consumerName = getTestConsumerName();
        connection.setProducer(getTestProducerName());
        connection.setConsumer(consumerName);

        connection.Produce(topic,"testKey","testValue");
        connection.Consume(topic);

        connection.Produce(topic2, "testKey2", "testValue2");
        connection.Produce(topic2, "testKey3", "testValue3");
        connection.Consume(topic2);

        connection.SaveOffsetInfo();

        connection.setConsumer(consumerName);

        var topics = connection.GetSubscribedTopics();

        assertThat(topics,hasItems(topic,topic2));
        var consumeResult = connection.Consume(topic);
        assertThat(consumeResult.iterator().hasNext(), is(false));

        connection.DeleteTopic(topic);
        connection.DeleteTopic(topic2);
    }

//    @Test
//    public void TestDeleteRecord() {
//        String topic = getTestTopicName();
//        if(connection.CheckTopic(topic)) {
//            connection.DeleteTopic(topic);
//        }
//        connection.CreateTopic(topic);
//        connection.setProducer(getTestProducerName());
//        connection.setConsumer(getTestConsumerName());
//
//
//        connection.Produce(topic,"testKey","testValue");
//        connection.DeleteRecord();
//        var result= connection.Consume(topic);
//
//        assertThat(result.iterator().hasNext(), is(false));
//
//        connection.DeleteTopic(topic);
//    }
}
