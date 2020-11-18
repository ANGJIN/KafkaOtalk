import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class ConsumerTest {

  @Test
  public void testConsume() throws ExecutionException, InterruptedException {
    String value = Double.toString(Math.random()*100%100);
    Producer.Produce("testTopic", "testKey", value);
    Thread.sleep(1000);

    ConsumerRecords<String, String> consumerRecords = Consumer.Consume("testTopic");

    List<ConsumerRecord<String, String>> recordsList = consumerRecords
        .records(new TopicPartition("testTopic", 0));
    ConsumerRecord<String,String> lastRecord = recordsList.get(recordsList.size()-1);

    assertThat(lastRecord.value(), is(value));
  }
}
