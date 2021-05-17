package services.scalable.scheduler

import akka.actor.ActorSystem
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchType}
import com.datastax.oss.driver.api.core.{CqlSession, DefaultConsistencyLevel}
import com.google.protobuf.any.Any
import io.vertx.core.Vertx
import io.vertx.kafka.client.common.TopicPartition
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.slf4j.LoggerFactory
import services.scalable.scheduler.protocol._

import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.jdk.FunctionConverters._
import java.util.concurrent.ThreadLocalRandom
import java.util.{Timer, UUID}
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class Aggregator()(implicit val ec: ExecutionContext) {

  val system = ActorSystem.create()
  implicit val provider = system.classicSystem

  val logger = LoggerFactory.getLogger(this.getClass)

  logger.info(s"\n${Console.MAGENTA_B}STARTING GLOBAL AGGREGATOR...${Console.RESET}\n")

  val rand = ThreadLocalRandom.current()

  val queue = TrieMap.empty[String, MetaBatch]

  val session = CqlSession
    .builder()
    //.addContactPoint(new InetSocketAddress(Config.CASSANDRA_HOST, Config.CASSANDRA_PORT))
    .withConfigLoader(loader)
    //.withLocalDatacenter(Config.DC)
    .withKeyspace(Config.KEYSPACE)
    .build()

  val INSERT_EPOCH = session.prepare("insert into batches(id, workers, completed, votes) values(?, ?, false, {});")

  val UPDATE_PARTITION_META = session.prepare(s"update partition_meta set last_offset = ? where topic = ? and p = ?;")
  val READ_PARTITION_META = session.prepare(s"select last_offset from partition_meta where topic=? and p=?;")

  def updatePartitionMetas(pms: Seq[PartitionMeta]): Future[Boolean] = {
    val stm = BatchStatement.builder(BatchType.LOGGED)

    pms.foreach { pm =>
      stm.addStatement(UPDATE_PARTITION_META
        .bind()
        .setLong(0, pm.offset)
        .setString(1, pm.topic)
        .setInt(2, pm.partition)
      )
    }

    session.executeAsync(stm.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM).build()).map(_.wasApplied())
  }

  def getPartitionMeta(topic: String, p: Int): Future[Long] = {
    session.executeAsync(READ_PARTITION_META.bind().setString(0, topic).setInt(1, p)).map { rs =>
      val one = rs.one()
      if(one != null) one.getLong("last_offset") else 0L
    }
  }

  def getPartitionMetas(pms: Seq[(String, Int)]): Future[Seq[(String, Int, Long)]] = {
    Future.sequence(pms.map{pm => getPartitionMeta(pm._1, pm._2).map{offset => Tuple3(pm._1, pm._2, offset)}})
  }

  val vertx = Vertx.vertx(voptions)

  val consumerConfig = scala.collection.mutable.Map[String, String]()

  consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, s"aggregator-consumer")
  /*consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, s"aggregator-consumer")
  consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")*/
  consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")

  val consumer = KafkaConsumer.create[String, Array[Byte]](vertx, consumerConfig.asJava)

  /*val producerConfig = scala.collection.mutable.Map[String, String]()

  producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerConfig.put(ProducerConfig.ACKS_CONFIG, "all")
  producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, s"aggregator-producer")
  producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

  val producer = KafkaProducer.create[String, Array[Byte]](vertx, producerConfig.asJava)*/

  val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers("localhost:9092")

  val kafkaProducer = producerSettings.createKafkaProducer()
  val settingsWithProducer = producerSettings.withProducer(kafkaProducer)

  val offsets = TrieMap.empty[Int, Long]
  val partitions = TrieMap.empty[Int, TopicPartition]

  def log(list: Seq[PartitionMeta]): Future[Boolean] = {
    if (list.isEmpty) return Future.successful(true)

    logger.info(s"\n${Console.YELLOW_B}MAXES: ${partitions.map{case (_, p) => offsets(p.getPartition())}}\n${Console.RESET}")

    val records = list.map { pm =>
      val buf = Any.pack(pm).toByteArray
      new ProducerRecord(Topics.LOG, pm.id, buf)
    }

    Source(records).runWith(Producer.plainSink(settingsWithProducer)).map(_ => true)
  }

  def getOffsets(jid: java.lang.Long = 0L): Unit = {
    var list = Seq.empty[(Int, java.lang.Long)]

    Future.sequence(partitions.map { case (_, t) => consumer.endOffsets(t).toCompletionStage.asScala.map(t.getPartition() -> _) }).flatMap { positions =>

      list = positions.filter { case (p, pos) => pos > offsets(p)}.toSeq
      //.map{case (p, pos) => p -> (Math.min(offsets(p) + Config.MAX_SCHEDULER_POLL_RECORDS, pos))}.toSeq

      // Sorting allows for priority execution ;)
      val tms = list.map { case (p, offset) => PartitionMeta(UUID.randomUUID.toString, Topics.METAS, Topics.HANDLERS, p, offset, offsets(p))}
        .sortBy{pm => pm.topic+pm.partition}

      //list.foreach { case (p, offset) => offsets.update(p, offset) }

      //save(tms).flatMap{ok => if(ok) log(tms) else Future.successful(false)}

      updatePartitionMetas(tms).flatMap { ok =>
        if(ok) {
          log(tms)
        } else {
          Future.successful(false)
        }
      }
    }.onComplete {
      case Success(ok) =>

        if(ok) {
          list.foreach { case (p, offset) => offsets.update(p, offset) }
        }

        vertx.setTimer(Config.AGGREGATOR_INTERVAL, getOffsets)

      case Failure(ex) => throw ex
    }
  }

  consumer.partitionsFor(Topics.METAS).toCompletionStage.asScala.onComplete {
    case Success(list) =>

      list.asScala.foreach { pinfo =>
        val partition = new TopicPartition(pinfo.getTopic, pinfo.getPartition)
        partitions.put(partition.getPartition, partition)
      }

      getPartitionMetas(partitions.values.map{p => p.getTopic() -> p.getPartition()}.toSeq).onComplete {
        case Success(positions) =>

          logger.info(s"${Console.YELLOW_B}positions: ${positions}${Console.RESET}")

          positions.foreach { case (topic, p, offset) =>
            offsets.put(p, offset)
          }

          vertx.setTimer(Config.AGGREGATOR_INTERVAL, getOffsets)

        case Failure(ex) => throw ex
      }

    case Failure(ex) => throw ex
  }

  def closeAll(): Unit = {
    logger.info(s"\n${Console.RED_B}STOPPING GLOBAL AGGREGATOR...${Console.RESET}\n")

    session.close()
    //producer.close()
    consumer.close()
    vertx.close()
  }

  /*Runtime.getRuntime.addShutdownHook (new Thread {
    closeAll()
  })*/

}