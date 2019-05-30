package io.confluent.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Optional.ofNullable;

/**
 * Helper class contains
 */
public class Serdes {

  public static SpecificAvroSerde<Movie> getMovieAvroSerde(Properties envProps) {
    final SpecificAvroSerde<Movie> movieSerde = new SpecificAvroSerde<>();
    movieSerde.configure(getSerdeConfig(envProps), false);
    return movieSerde;
  }

  public static SpecificAvroSerde<RatedMovie> getRatedMovieAvroSerde(Properties envProps) {
    SpecificAvroSerde<RatedMovie> ratedMovieSerde = new SpecificAvroSerde<>();
    ratedMovieSerde.configure(getSerdeConfig(envProps), false);
    return ratedMovieSerde;
  }

  protected static Map<String, String> getSerdeConfig(Properties config) {
    final String srUserInfoPropertyName = "schema.registry.basic.auth.user.info";
    final HashMap<String, String> map = new HashMap<>();

    final String srUrlConfig = config.getProperty(SCHEMA_REGISTRY_URL_CONFIG);
    final String srAuthCredsConfig = config.getProperty(BASIC_AUTH_CREDENTIALS_SOURCE);
    final String srUserInfoConfig = config.getProperty(srUserInfoPropertyName);

    map.put(SCHEMA_REGISTRY_URL_CONFIG, ofNullable(srUrlConfig).orElse(""));
    map.put(BASIC_AUTH_CREDENTIALS_SOURCE, ofNullable(srAuthCredsConfig).orElse(""));
    map.put(srUserInfoPropertyName, ofNullable(srUserInfoConfig).orElse(""));
    return map;
  }
}
