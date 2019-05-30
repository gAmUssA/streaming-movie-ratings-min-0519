package io.confluent.demo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import io.confluent.devx.kafka.config.ConfigLoader;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static io.confluent.demo.Serdes.getMovieAvroSerde;
import static io.confluent.demo.Serdes.getRatedMovieAvroSerde;
import static io.confluent.devx.kafka.streams.TopologyVisualizer.visualize;
import static java.lang.Integer.parseInt;
import static java.lang.Short.parseShort;
import static org.apache.kafka.common.serialization.Serdes.Double;
import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.apache.kafka.streams.StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.CONSUMER_PREFIX;
import static org.apache.kafka.streams.StreamsConfig.PRODUCER_PREFIX;
import static org.apache.kafka.streams.StreamsConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.apache.kafka.streams.StreamsConfig.topicPrefix;

public class StreamsDemo {

  //region buildStreamsProperties
  protected Properties buildStreamsProperties(Properties envProps) {
    Properties config = new Properties();

    config.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Long().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Double().getClass());
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, envProps.getProperty("schema.registry.url"));

    config.put(REPLICATION_FACTOR_CONFIG, envProps.getProperty("default.topic.replication.factor"));

    config.put(PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
               "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

    config.put(CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
               "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, envProps.getProperty("offset.reset.policy"));

    // config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    // Enable record cache of size 2 MB.
    config.put(CACHE_MAX_BYTES_BUFFERING_CONFIG, 2 * 1024 * 1024L);
    // Set commit interval to 1 second.
    config.put(COMMIT_INTERVAL_MS_CONFIG, 1000);
    config.put(topicPrefix("segment.ms"), 15000000);

    // from https://docs.confluent.io/current/cloud/connect/streams-cloud-config.html
    // Recommended performance/resilience settings
    config.put(producerPrefix(ProducerConfig.RETRIES_CONFIG), 2147483647);
    config.put("producer.confluent.batch.expiry.ms", 9223372036854775807L);
    config.put(producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 300000);
    config.put(producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), 9223372036854775807L);

    return config;
  }
  //endregion

  //region createTopics

  /**
   * Create topics using AdminClient API
   */
  private void createTopics(Properties envProps) {
    Map<String, Object> config = new HashMap<>();

    config.put("bootstrap.servers", envProps.getProperty("bootstrap.servers"));
    AdminClient client = AdminClient.create(config);

    List<NewTopic> topics = new ArrayList<>();

    topics.add(new NewTopic(
        envProps.getProperty("input.movies.topic.name"),
        parseInt(envProps.getProperty("input.movies.topic.partitions")),
        parseShort(envProps.getProperty("input.movies.topic.replication.factor"))));

    topics.add(new NewTopic(
        envProps.getProperty("input.ratings.topic.name"),
        parseInt(envProps.getProperty("input.ratings.topic.partitions")),
        parseShort(envProps.getProperty("input.ratings.topic.replication.factor"))));

    topics.add(new NewTopic(
        envProps.getProperty("output.movies.topic.name"),
        parseInt(envProps.getProperty("output.movies.topic.partitions")),
        parseShort(envProps.getProperty("output.movies.topic.replication.factor"))));

    topics.add(new NewTopic(
        envProps.getProperty("output.rating-sum.topic.name"),
        parseInt(envProps.getProperty("output.rating-sum.topic.partitions")),
        parseShort(envProps.getProperty("output.rating-sum.topic.replication.factor"))));

    topics.add(new NewTopic(
        envProps.getProperty("output.rating-sum.topic.name"),
        parseInt(envProps.getProperty("output.rating-sum.topic.partitions")),
        parseShort(envProps.getProperty("output.rating-sum.topic.replication.factor"))));

    topics.add(new NewTopic(
        envProps.getProperty("output.rating-counts.topic.name"),
        parseInt(envProps.getProperty("output.rating-counts.topic.partitions")),
        parseShort(envProps.getProperty("output.rating-counts.topic.replication.factor"))));

    topics.add(new NewTopic(
        envProps.getProperty("output.rating-averages.topic.name"),
        parseInt(envProps.getProperty("output.rating-averages.topic.partitions")),
        parseShort(envProps.getProperty("output.rating-averages.topic.replication.factor"))));

    topics.add(new NewTopic(
        envProps.getProperty("output.rated-movies.topic.name"),
        parseInt(envProps.getProperty("output.rated-movies.topic.partitions")),
        parseShort(envProps.getProperty("output.rated-movies.topic.replication.factor"))));

    client.createTopics(topics);
    client.close();

  }
  //endregion

  //region buildTopology
  private Topology buildTopology(StreamsBuilder builder,
                                 Properties envProps) {

    final String rawRatingTopicName = envProps.getProperty("input.ratings.topic.name");
    final String rawMoviesTopicName = envProps.getProperty("input.movies.topic.name");
    final String avroMoviesTopicName = envProps.getProperty("output.movies.topic.name");
    final String ratedMoviesTopicName = envProps.getProperty("output.rated-movies.topic.name");
    final String avgRatingsTopicName = envProps.getProperty("output.rating-averages.topic.name");

    // Movies processors
    SpecificAvroSerde<Movie> movieSerde = getMovieAvroSerde(envProps);
    final KTable<Long, Movie> movies = getMoviesTable(builder,
                                                      rawMoviesTopicName,
                                                      avroMoviesTopicName,
                                                      movieSerde);

    // Ratings processor
    KStream<Long, String> rawRatingsStream = getRawRatingsStream(builder, rawRatingTopicName);
    KTable<Long, Double> ratingAverage = getRatingAverageTable(rawRatingsStream, avgRatingsTopicName);

    SpecificAvroSerde<RatedMovie> ratedMovieSerde = getRatedMovieAvroSerde(envProps);
    getRatedMoviesTable(movies, ratingAverage, ratedMoviesTopicName, ratedMovieSerde);

    // finish the topology
    return builder.build();
  }
  //endregion

  protected static KTable<Long, Movie> getMoviesTable(StreamsBuilder builder,
                                                      String rawMoviesTopicName,
                                                      String avroMoviesTopicName,
                                                      SpecificAvroSerde<Movie> movieSerde) {

    final KStream<Long, String> rawMovies =
        builder.stream(rawMoviesTopicName,
                       Consumed.with(Long(), String()));

    // Parsed movies
    rawMovies
        .mapValues(Parser::parseMovie)
        .map((key, movie) -> new KeyValue<>(movie.getMovieId(), movie))
        .to(avroMoviesTopicName, Produced.with(Long(), movieSerde));

    // Movies table
    return builder.table(avroMoviesTopicName,
                         Materialized.<Long, Movie, KeyValueStore<Bytes, byte[]>>as(avroMoviesTopicName + "-store")
                             .withValueSerde(movieSerde)
                             .withKeySerde(Long()));
  }

  protected static KTable<Long, Double> getRatingAverageTable(KStream<Long, String> rawRatings,
                                                              String avgRatingsTopicName) {

    KStream<Long, Rating> ratings =
        rawRatings
            .mapValues(Parser::parseRating)
            .map((key, rating) -> new KeyValue<>(rating.getMovieId(), rating));

    // Parsing Ratings
    KGroupedStream<Long, Double> ratingsById =
        ratings
            .mapValues(Rating::getRating)
            .groupByKey();

    KTable<Long, Long> count = ratingsById.count();
    KTable<Long, Double>
        sumTable = ratingsById.reduce(Double::sum,
                                      Materialized.with(Long(), Double()));

    final KTable<Long, Double> join = sumTable.join(count,
                                                    (sum, count1) -> sum / count1,
                                                    Materialized.as(avgRatingsTopicName));
    // persist the result in topic
    join.toStream().to(avgRatingsTopicName);
    return join;
  }

  protected static KStream<Long, String> getRawRatingsStream(StreamsBuilder builder,
                                                             String rawRatingTopicName) {
    return builder.stream(rawRatingTopicName,
                          Consumed.with(Long(),
                                        String()));
  }

  public static KTable<Long, RatedMovie> getRatedMoviesTable(KTable<Long, Movie> movies,
                                                             KTable<Long, Double> ratingAverage,
                                                             String ratedMovieTopic,
                                                             SpecificAvroSerde<RatedMovie> ratedMovieSerde) {

    ValueJoiner<Double, Movie, RatedMovie> joiner = (avg, movie) -> new RatedMovie(movie.getMovieId(),
                                                                                   movie.getTitle(),
                                                                                   movie.getReleaseYear(),
                                                                                   avg);
    KTable<Long, RatedMovie>
        ratedMovies =
        ratingAverage
            .join(movies, joiner, Materialized.<Long, RatedMovie, KeyValueStore<Bytes, byte[]>>as("rated-movies-store")
                .withValueSerde(ratedMovieSerde)
                .withKeySerde(Long()));

    ratedMovies.toStream().to(ratedMovieTopic, Produced.with(Long(), ratedMovieSerde));
    return ratedMovies;
  }

  private Properties loadEnvProperties(String fileName) {
    return ConfigLoader.loadConfig(fileName);
  }

  private void run(String configPath) {
    Properties envProps = this.loadEnvProperties(configPath);
    Properties streamProps = this.buildStreamsProperties(envProps);
    Topology topology = this.buildTopology(new StreamsBuilder(), envProps);

    this.createTopics(envProps);

    System.out.println(visualize(topology.describe().toString()));

    final KafkaStreams streams = new KafkaStreams(topology, streamProps);
    final CountDownLatch latch = new CountDownLatch(1);

    // Attach shutdown handler to catch Control-C.
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        streams.close();
        latch.countDown();
      }
    });

    try {
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      throw new IllegalArgumentException(
          "🙀 This program takes one argument: the path to an environment configuration file.");
    }

    new StreamsDemo().run(args[0]);
  }
}
