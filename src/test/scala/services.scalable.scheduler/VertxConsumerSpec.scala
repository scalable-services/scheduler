package services.scalable.scheduler

import io.vertx.core.{Vertx, VertxOptions}
import io.vertx.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.io.StdIn
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.util.{Failure, Success}

class VertxConsumerSpec extends AnyFlatSpec {

  "it " should " consume records successfully " in {

    val voptions = new VertxOptions()
      .setMaxWorkerExecuteTime(5000L)
      .setMaxEventLoopExecuteTime(5000L)

    val vertx = Vertx.vertx(voptions)

    val consumerConfig = scala.collection.mutable.Map[String, String]()

    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "vertx-g0-consumer")
    consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, "vertx-app-consumer")
    consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")

    val consumer = KafkaConsumer.create[String, Array[Byte]](vertx, consumerConfig.asJava)

    consumer.subscribe("demo")

    def handler(recs: KafkaConsumerRecords[String, Array[Byte]]): Unit = {
      consumer.pause()

      for(i<-0 until recs.size()){
        val rec = recs.recordAt(i)
        println(rec.key() -> new String(rec.value()))
      }

      consumer.commitScala().onComplete {
        case Success(ok) => consumer.resume()
        case Failure(ex) => throw ex
      }
    }

    consumer.handler(_ => {})
    consumer.batchHandler(handler)

    StdIn.readLine()

    consumer.close()

    //consumer.assign(new TopicPartition("demo", 0))
  }

}
