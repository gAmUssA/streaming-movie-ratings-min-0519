import io.confluent.devx.kafka.config.ConfigLoader
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringSerializer

class MovieLoader {

  static void main(args) {

    if (args.length < 1) {
      throw new IllegalArgumentException(
          "🙀 This program takes one argument: the path to an environment configuration file.")
    }
    Properties props = ConfigLoader.loadConfig(args[0] as String)

    props.load(new FileInputStream(new File(args[0])))

    props.put('key.serializer', LongSerializer.class.getName())
    props.put('value.serializer', StringSerializer.class.getName())

    KafkaProducer producer = new KafkaProducer(props)

    try {
      long currentTime = System.currentTimeSeconds()
      println currentTime

      def movieFile = new File(props.get('movies.file') as String)
      movieFile.eachLine { line ->
        println line
        def pr = new ProducerRecord(props.get('input.movies.topic.name') as String,
                                    null,
                                    line)
        producer.send(pr)
      }
    }
    finally {
      producer.close()
    }
  }
}
