import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ProducerTest {

  @Test
  public void testProduce() throws ExecutionException, InterruptedException {
    String value = Double.toString(Math.random() * 100 % 100);
    RecordMetadata metadata = Producer.Produce("testTopic", "testKey", value);
    assertThat(metadata.topic(), is("testTopic"));
    assertThat(metadata.serializedKeySize(), is(7));
    assertThat(metadata.serializedValueSize(), is(value.length()));
  }
}
