package services.scalable.scheduler

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.{CommitDelivery, CommitterSettings, ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.stream.KillSwitches
import akka.stream.impl.Cancel
import akka.stream.scaladsl.{Keep, Sink}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

class AkkaConsumerSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  "it " should " consume records successfully " in {

    val system = ActorSystem.create()
    implicit val provider = system.classicSystem
    val config = system.settings.config.getConfig("akka.kafka.consumer")

    val consumerSettings = ConsumerSettings[String, Array[Byte]](system, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers("localhost:9092")
      /*.withGroupId("g25")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")*/
      .withClientId("c25")

    def business(key: String, value: Array[Byte]): Future[String] = {
      logger.info(s"\n${Console.GREEN_B}tuple: ${key} = ${new String(value)}${Console.RESET}\n")
      Future.successful(key)
    }

    val committerSettings = CommitterSettings(system).withDelivery(CommitDelivery.waitForAck)

    /*val control =
      Consumer
        .committableSource(consumerSettings, Subscriptions.topics("test"))
        .mapAsync(1) { msg =>
          business(msg.record.key, msg.record.value).map(_ => msg.committableOffset)
        }
        .via(Committer.flow(committerSettings.withMaxBatch(1)))
        //.toMat(Sink.seq)(DrainingControl.apply)
        .runWith(Sink.seq)

      val seq = Await.result(control, Duration.Inf)*/

    /*val simpleControl = Consumer.plainSource(consumerSettings, Subscriptions.topics("test"))
      .mapAsync(1){ msg =>
        println(msg.key() -> new String(msg.value()))
        Future.successful(Done.done())
      }
      .runWith(Sink.ignore)

    Await.result(simpleControl, Duration.Inf)*/

    // Range...
    val control = Consumer.plainSource(consumerSettings, Subscriptions.assignmentWithOffset(new TopicPartition("test", 0) -> 10L))
      .mapAsync(1){ msg =>
        val tuple = msg.offset() -> msg.key() -> new String(msg.value())
        println(tuple)
        Future.successful(tuple)
      }
      .take(10)

      //Condition based...
      //.takeWhile()
      .runWith(Sink.seq)

    val result = Await.result(control, Duration.Inf)

    println(result)
  }

}
