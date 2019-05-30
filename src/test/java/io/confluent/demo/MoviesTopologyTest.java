package io.confluent.demo;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;

import static io.confluent.demo.fixture.MoviesAndRatingsData.DUMMY_KAFKA_CONFLUENT_CLOUD_9092;
import static io.confluent.demo.fixture.MoviesAndRatingsData.DUMMY_SR_CONFLUENT_CLOUD_8080;
import static io.confluent.demo.fixture.MoviesAndRatingsData.LETHAL_WEAPON_MOVIE;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertNotNull;

@Slf4j
public class MoviesTopologyTest {

  private static final String RAW_MOVIES_TOPIC_NAME = "raw-movies";
  private TopologyTestDriver td;

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
    
    // workaround https://stackoverflow.com/a/50933452/27563
    streamsConfig.setProperty(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams/" + LocalDateTime.now().toString());

    StreamsBuilder builder = new StreamsBuilder();

    SpecificAvroSerde<Movie> movieSerde = new SpecificAvroSerde<>(new MockSchemaRegistryClient());
    movieSerde.configure(singletonMap(SCHEMA_REGISTRY_URL_CONFIG, DUMMY_SR_CONFLUENT_CLOUD_8080), false);
    StreamsDemo.getMoviesTable(builder, RAW_MOVIES_TOPIC_NAME, "movies", movieSerde);

    final Topology topology = builder.build();
    log.info("Topology: \n{}", topology.describe());
    td = new TopologyTestDriver(topology, streamsConfig);
  }

  @Test
  public void validateIfTestDriverCreated() {
    assertNotNull(td);
  }

  @Test
  public void validateAvroMovie() {
    ConsumerRecordFactory<Long, String> rawMovieRecordFactory =
        new ConsumerRecordFactory<>(RAW_MOVIES_TOPIC_NAME, new LongSerializer(),
                                    new StringSerializer());

    td.pipeInput(rawMovieRecordFactory.create(LETHAL_WEAPON_MOVIE));

    //td.readOutput("")
    final KeyValueStore<Long, Movie> movieStore =
        td.getKeyValueStore("movies-store");
    final Movie movie = movieStore.get(362L);
    Assert.assertEquals("Lethal Weapon", movie.getTitle());

  }
}
