package io.confluent.demo;

import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import static io.confluent.demo.StreamsDemo.getRatingAverageTable;
import static io.confluent.demo.StreamsDemo.getRawRatingsStream;
import static io.confluent.demo.fixture.MoviesAndRatingsData.DUMMY_KAFKA_CONFLUENT_CLOUD_9092;
import static io.confluent.demo.fixture.MoviesAndRatingsData.DUMMY_SR_CONFLUENT_CLOUD_8080;
import static io.confluent.demo.fixture.MoviesAndRatingsData.LETHAL_WEAPON_RATING_10;
import static io.confluent.demo.fixture.MoviesAndRatingsData.LETHAL_WEAPON_RATING_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

@Slf4j
public class RatingTopologiesTest {

  private static final String RAW_RATINGS_TOPIC_NAME = "raw-ratings";
  private static final String AVERAGE_RATINGS_TOPIC_NAME = "average-ratings";
  private TopologyTestDriver testDriver;

  @Before
  public void setUp() {

    final Properties properties = new Properties();
    properties.put("application.id", "kafka-movies-test");
    properties.put("bootstrap.servers",DUMMY_KAFKA_CONFLUENT_CLOUD_9092);
    properties.put("schema.registry.url", DUMMY_SR_CONFLUENT_CLOUD_8080);
    properties.put("default.topic.replication.factor", "1");
    properties.put("offset.reset.policy", "latest");

    final StreamsDemo streamsApp = new StreamsDemo();
    final Properties streamsConfig = streamsApp.buildStreamsProperties(properties);

    StreamsBuilder builder = new StreamsBuilder();
    final KStream<Long, String> rawRatingsStream = getRawRatingsStream(builder, RAW_RATINGS_TOPIC_NAME);
    final KTable<Long, Double> ratingAverageTable = getRatingAverageTable(rawRatingsStream, "average-ratings");

    final Topology topology = builder.build();
    log.info("topology = \n" + topology.describe());
    testDriver = new TopologyTestDriver(topology, streamsConfig);

  }

  @Test
  public void validateIfTestDriverCreated() {
    assertNotNull(testDriver);
  }

  @Test
  public void validateAverageRating() {
    final TestInputTopic<Long, String>
        inputTopic =
        testDriver.createInputTopic(RAW_RATINGS_TOPIC_NAME, new LongSerializer(), new StringSerializer());

    // Lethal Weapon ratings
    inputTopic.pipeValueList(Arrays.asList(LETHAL_WEAPON_RATING_10, LETHAL_WEAPON_RATING_8));

    final TestOutputTopic<Long, Double>
        outputTopic =
        testDriver.createOutputTopic(AVERAGE_RATINGS_TOPIC_NAME, new LongDeserializer(), new DoubleDeserializer());
    final List<KeyValue<Long, Double>> longDoubleKeyValue = outputTopic.readKeyValuesToList();
    assertThat(longDoubleKeyValue,
               equalTo(new KeyValue<>(362L, 9.0)));

    final KeyValueStore<Long, Double>
        keyValueStore =
        testDriver.getKeyValueStore("average-ratings");
    final Double expected = keyValueStore.get(362L);
    Assert.assertEquals("Message", expected, 9.0, 0.0);
  }

  @After
  public void tearDown() {
    testDriver.close();
  }
}