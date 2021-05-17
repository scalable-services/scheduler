package services.scalable.scheduler

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.{LongSerde, StringSerde}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{KStream, KTable, Materialized, Named, ValueMapper}
import org.apache.kafka.streams.state.{KeyValueStore, SessionStore}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.FutureConverters._
import scala.jdk.FunctionConverters._
import scala.jdk.CollectionConverters._
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

class KafkaStreamsConsumer extends AnyFlatSpec {

  "it " should " produce successfully " in {

    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p
    }

    val builder: StreamsBuilder = new StreamsBuilder
    val textLines: KStream[String, String] = builder.stream[String, String]("test")

    val wordCounts: KTable[String, java.lang.Long] = textLines.flatMapValues(s => util.Arrays.asList(s.split("\\W+")))
      .groupBy{(k, w) => k}
      .count(Materialized.as[String, java.lang.Long, KeyValueStore[Bytes, Array[Byte]]]("count-words"))

    wordCounts.toStream.to("words")

    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close()
    }
  }

}
