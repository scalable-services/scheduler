package services.scalable.scheduler

import io.vertx.core.{Vertx, VertxOptions}
import io.vertx.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.commons.lang3.RandomStringUtils
import org.apache.kafka.clients.producer.ProducerConfig
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.jdk.CollectionConverters._
import scala.concurrent.ExecutionContext.Implicits.global

class VertxProducerSpec extends AnyFlatSpec {

  "it " should " produce records successfully " in {

    val voptions = new VertxOptions()
      .setMaxWorkerExecuteTime(5000L)
      .setMaxEventLoopExecuteTime(5000L)

    val vertx = Vertx.vertx(voptions)

    val producerConfig = scala.collection.mutable.Map[String, String]()

    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all")
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "vertx-app-producer")
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    val producer = KafkaProducer.create[String, Array[Byte]](vertx, producerConfig.asJava)

    for(i<-0 until 100){
      val k = RandomStringUtils.randomAlphanumeric(4, 10)
      val v = RandomStringUtils.randomAlphabetic(1, 5).getBytes()

      val record = KafkaProducerRecord.create[String, Array[Byte]]("test", k, v)

      producer.write(record)
    }

    val r = Await.result(producer.flush(), Duration.Inf)

    println(r)
  }

}
