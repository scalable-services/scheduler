package services.scalable.scheduler

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.commons.lang3.RandomStringUtils
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class AkkaProducerSpec extends AnyFlatSpec {

  "it " should " produce records successfully " in {

    val system = ActorSystem.create()
    implicit val provider = system.classicSystem
    val config = system.settings.config.getConfig("akka.kafka.producer")

    val bootstrapServers = "localhost:9092"

    val producerSettings = ProducerSettings(config, new StringSerializer, new StringSerializer)
        .withBootstrapServers(bootstrapServers)

    val topic = "test"

    val done: Future[Done] = Source(1 to 100)
        .map(_ => RandomStringUtils.randomAlphabetic(3, 5) -> RandomStringUtils.randomAlphabetic(3, 5))
        .map(tuple => new ProducerRecord[String, String](topic, tuple._1, tuple._2))
        .runWith(Producer.plainSink(producerSettings))

    val result = Await.result(done, Duration.Inf)

    println(result)
  }

}
