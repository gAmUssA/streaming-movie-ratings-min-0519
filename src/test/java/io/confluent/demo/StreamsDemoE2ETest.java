package io.confluent.demo;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;

import static io.confluent.demo.StreamsDemo.getMoviesTable;
import static io.confluent.demo.StreamsDemo.getRatedMoviesTable;
import static io.confluent.demo.StreamsDemo.getRatingAverageTable;
import static io.confluent.demo.StreamsDemo.getRawRatingsStream;
import static io.confluent.demo.fixture.MoviesAndRatingsData.DUMMY_KAFKA_CONFLUENT_CLOUD_9092;
import static io.confluent.demo.fixture.MoviesAndRatingsData.DUMMY_SR_CONFLUENT_CLOUD_8080;
import static io.confluent.demo.fixture.MoviesAndRatingsData.LETHAL_WEAPON_MOVIE;
import static io.confluent.demo.fixture.MoviesAndRatingsData.LETHAL_WEAPON_RATING_9;

@Slf4j
@Ignore
public class StreamsDemoE2ETest {

  private static final String RAW_RATINGS_TOPIC_NAME = "raw-ratings";
  private static final String RAW_MOVIES_TOPIC_NAME = "raw-movies";
  private static final String RATED_MOVIES_TOPIC_NAME = "rated-movies";
  private TopologyTestDriver td;
  private SpecificAvroSerde<RatedMovie> ratedMovieSerde;


  @Before
  public void setUp() throws IOException {

    final Properties properties = new Properties();
    properties.put("application.id", "kafka-movies-test");
    properties.put("bootstrap.servers", DUMMY_KAFKA_CONFLUENT_CLOUD_9092);
    properties.put("schema.registry.url", DUMMY_SR_CONFLUENT_CLOUD_8080);
    properties.put("default.topic.replication.factor", "1");
    properties.put("offset.reset.policy", "latest");

    final StreamsDemo streamsApp = new StreamsDemo();
    final Properties streamsConfig = streamsApp.buildStreamsProperties(properties);

    // workaround https://stackoverflow.com/a/50933452/27563
    final String tempDirectory = Files.createTempDirectory("kafka-streams")
        .toAbsolutePath()
        .toString();
    streamsConfig.setProperty(StreamsConfig.STATE_DIR_CONFIG, tempDirectory);

    final Map<String, String> mockSerdeConfig = Serdes.getSerdeConfig(streamsConfig);

    //building topology
    StreamsBuilder builder = new StreamsBuilder();

    SpecificAvroSerde<Movie> movieSerde = new SpecificAvroSerde<>(new MockSchemaRegistryClient());
    movieSerde.configure(mockSerdeConfig, false);

    ratedMovieSerde = new SpecificAvroSerde<>(new MockSchemaRegistryClient());
    ratedMovieSerde.configure(mockSerdeConfig, false);

    final KStream<Long, String> rawRatingsStream = getRawRatingsStream(builder, "raw-ratings");
    SpecificAvroSerde<CountAndSum> countAndSumSerde = new SpecificAvroSerde<>(new MockSchemaRegistryClient());
    countAndSumSerde.configure(mockSerdeConfig, false);
    
    final KTable<Long, Double> ratingAverageTable = getRatingAverageTable(rawRatingsStream, "average-ratings",
                                                                          countAndSumSerde);

    final KTable<Long, Movie> moviesTable = getMoviesTable(builder,
                                                           "raw-movies",
                                                           "movies",
                                                           movieSerde);

    getRatedMoviesTable(moviesTable, ratingAverageTable, "rated-movies", ratedMovieSerde);

    final Topology topology = builder.build();
    log.info("Topology = \n{}", topology.describe());
    td = new TopologyTestDriver(topology, streamsConfig);
  }

  @Test
  public void validateRatingForLethalWeapon() {

    final TestInputTopic<Long, String>
        rawRatingsTopic =
        td.createInputTopic(RAW_RATINGS_TOPIC_NAME, new LongSerializer(), new StringSerializer());

    final TestInputTopic<Long, String>
        rawMoviesTopic =
        td.createInputTopic(RAW_MOVIES_TOPIC_NAME, new LongSerializer(), new StringSerializer());

    rawMoviesTopic.pipeValueList(Collections.singletonList(LETHAL_WEAPON_MOVIE));

    rawRatingsTopic
        .pipeValueList(Arrays.asList(LETHAL_WEAPON_RATING_9, LETHAL_WEAPON_RATING_9, LETHAL_WEAPON_RATING_9));

    final TestOutputTopic<Long, RatedMovie>
        outputTopic =
        td.createOutputTopic(RATED_MOVIES_TOPIC_NAME, new LongDeserializer(), ratedMovieSerde.deserializer());
    final List<KeyValue<Long, RatedMovie>> result = outputTopic.readKeyValuesToList();

    result.forEach(record -> {
      ProducerRecord<Long, RatedMovie>
          lethalWeaponRecord =
          new ProducerRecord<>(RATED_MOVIES_TOPIC_NAME, 362L, new RatedMovie(362L, "Lethal Weapon", 1987, 9.0));
      Assert.assertThat(record.value, CoreMatchers.equalTo(lethalWeaponRecord.value()));
    });
    
  }

}
