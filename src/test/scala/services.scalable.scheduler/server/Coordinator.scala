package services.scalable.scheduler.server

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka.{CommitDelivery, CommitterSettings, ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BatchType}
import com.datastax.oss.driver.api.core.{CqlSession, DefaultConsistencyLevel}
import com.google.protobuf.any.Any
import io.vertx.core.Vertx
import io.vertx.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.commons.lang3.RandomStringUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory
import services.scalable.scheduler.protocol._
import services.scalable.scheduler._

import scala.jdk.FutureConverters._
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.util.{Failure, Success}
import scala.concurrent.duration._

class Coordinator(val name: String, val id: Int, val port: Int)(implicit ec: ExecutionContext) extends CoordinatorService {

  val system = ActorSystem.create()
  implicit val provider = system.classicSystem

  val logger = LoggerFactory.getLogger(this.getClass)

  val queue = TrieMap.empty[String, (Command, Promise[TaskResponse])]
  val processing = TrieMap.empty[String, (Command, Promise[TaskResponse])]

  val vertx = Vertx.vertx(voptions)

  val session = CqlSession
    .builder()
    .addContactPoint(new InetSocketAddress(Config.CASSANDRA_HOST, Config.CASSANDRA_PORT))
    .withConfigLoader(loader)
    .withLocalDatacenter(Config.DC)
    .withKeyspace(Config.KEYSPACE)
    .build()

  val producerSettings = ProducerSettings(system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers("localhost:9092")

  val kafkaProducer = producerSettings.createKafkaProducer()
  val settingsWithProducer = producerSettings.withProducer(kafkaProducer)

  val INSERT_COMMAND = session.prepare("insert into commands(command_id, batch_id, data, completed) values(?, ?, ?, false);")
  val INSERT_META_BATCH = session.prepare("insert into batches(id, workers, completed, votes) values(?, ?, false, {});")

  val INSERT_SCHEDULER_COMMAND = session.prepare("insert into scheduler_commands(batch_id, scheduler, command_id, data) values(?, ?, ?, ?);")

  val COMPLETE_BATCH = session.prepare(s"update batches set completed=true where id=?;")

  val CHECK_COMPLETED = session.prepare(s"select * from batches where id=?;")

  def completeBatch(bid: String): Future[Boolean] = {
    session.executeAsync(COMPLETE_BATCH.bind().setString(0, bid)).map(_.wasApplied())
  }

  def checkBatch(bid: String): Future[(Boolean, Seq[String], Seq[String])] = {
    session.executeAsync(CHECK_COMPLETED.bind().setString(0, bid)).map { rs =>
      val one = rs.one()
      if(one != null) Tuple3(one.getBoolean("completed"),
        one.getSet("successes", classOf[String]).asScala.toSeq, one.getSet("failures", classOf[String]).asScala.toSeq)
      else Tuple3(false, Seq.empty[String], Seq.empty[String])
    }
  }

  def saveMetaBatch(b: PartitionMeta): Future[Boolean] = {
    session.executeAsync(INSERT_META_BATCH.bind().setString(0, b.id).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).map(_.wasApplied())
  }

  def saveMetaBatch(b: MetaBatch): Future[Boolean] = {
    val schedulers = b.schedulers.map(_.toString).toSet[String]
    session.executeAsync(INSERT_META_BATCH.bind().setString(0, b.id).setSet(1, schedulers.asJava, classOf[String])
      .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).map(_.wasApplied())
  }

  def saveBatch(b: Batch, m: MetaBatch): Future[Boolean] = {
    val stm = BatchStatement.builder(BatchType.LOGGED)

    stm.addStatement(INSERT_META_BATCH.bind().setString(0, b.id).setSet(1, m.schedulers.map(_.toString).toSet.asJava, classOf[String]))

    b.tasks.foreach { t =>
      stm.addStatement(INSERT_COMMAND.bind().setString(0, t.id).setString(1, b.id)
        .setByteBuffer(2, ByteBuffer.wrap(Any.pack(t).toByteArray)))
    }

    session.executeAsync(stm.setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM).build()).map(_.wasApplied())
  }

  def post(meta: MetaBatch): Future[Boolean] = {
    val buf = Any.pack(meta).toByteArray

    val record = new ProducerRecord(Topics.METAS, meta.id, buf)

    Source.single(record)
      .runWith(Producer.plainSink(settingsWithProducer)).map(_ => true)
  }

  val rand = ThreadLocalRandom.current()

  val batches = TrieMap.empty[String, Batch]

  def task(jid: java.lang.Long = 0L): Unit = {

    val commands = queue.map(_._2)

    if(commands.isEmpty){
      vertx.setTimer(Config.COORDINATOR_BATCH_INTERVAL, task)
      return
    }

    var keys = Seq.empty[String]
    val it = commands.iterator
    var tasks = Seq.empty[(Command, Promise[TaskResponse])]

    val batchId = UUID.randomUUID.toString()

    var n = 0

    while(it.hasNext /*&& n < Config.MAX_COORDINATOR_BUFFER_SIZE*/){
      val next = it.next()
      val (cmd, p) = next

      if(!cmd.writes.keys.exists{keys.contains(_)}){
        keys = (keys ++ cmd.reads.keys.toSeq.distinct)
        tasks = tasks :+ cmd.withBatchId(batchId) -> p

        n += 1

      } else {
        p.success(TaskResponse(cmd.id, false))
        queue.remove(cmd.id)
      }
    }

    if(tasks.isEmpty){
      vertx.setTimer(Config.COORDINATOR_BATCH_INTERVAL, task)
      return
    }

    //assert(!keys.exists{k => keys.count(_.equals(k)) > 1})

    val partitions = keys.map{computePartition(_)}.distinct
    val schedulers = keys.map{computeScheduler(_)}.distinct.sorted
    val scheduler = if(schedulers.length > 1) schedulers(rand.nextInt(0, schedulers.length)) else schedulers.head

    val batch = Batch(batchId, tasks.map(_._1))
    val meta = MetaBatch(batchId, Topics.METAS, Topics.HANDLERS, id, schedulers, scheduler, partitions, tasks.map(_._1))

    //processing2.put(batch.id, tasks)

    tasks.foreach { case (c, p) =>
      processing.put(c.id, c -> p)
    }

    logger.info(s"${Console.CYAN_B} $name CREATED BATCH ${batch.id} with schedulers with len ${batch.tasks.length} ${schedulers} with leader ${schedulers.head}${Console.RESET}")

    batches.put(batch.id, batch)

    saveBatch(batch, meta).flatMap{ok => if(ok) post(meta) else Future.successful(false)}.onComplete {
      case Success(ok) =>

        if(ok) {
          tasks.foreach{case (cmd, _) => queue.remove(cmd.id)}
        }

        vertx.setTimer(Config.COORDINATOR_BATCH_INTERVAL, task)

      case Failure(ex) => ex.printStackTrace()
    }

  }

  vertx.setTimer(10L, task)

  def checkCompleted(jid: java.lang.Long = 0L): Unit = {

    val all = batches.map(_._2).toSeq

    if(all.isEmpty){
      vertx.setTimer(10L, checkCompleted)
      return
    }

    val notcompleted = TrieMap.empty[String, Batch]

    Future.sequence(all.map{m => checkBatch(m.id).map{m -> _}}).onComplete {
      case Success(results) =>

        //logger.info(s"completed ${results.map(x => x._1.id -> x._2)}")

        /*val notcompleted = results.filter(_._2._1 == false)

        if(!notcompleted.isEmpty){
          logger.info(s"\n${Console.RED_B}not completed: ${results.map{case (r, success) => r.id -> success._1}}${Console.RESET}\n")
        }*/

        val x = results.filter{x => x._2._1 == false && !notcompleted.isDefinedAt(x._1.id)}.map { case (m, _) =>
          notcompleted.put(m.id, m)
          m.id
        }

        results.filter(_._2._1 == true).foreach { case (m, (_, succeed, failed)) =>

          notcompleted.remove(m.id)

          succeed.foreach { cid =>
            processing.remove(cid) match {
              case Some((c, p)) => p.success(TaskResponse(cid, true))
              case None =>
            }
          }

          failed.foreach { cid =>
            processing.remove(cid) match {
              case Some((c, p)) => p.success(TaskResponse(cid, false))
              case None =>
            }
          }

          batches.remove(m.id)
        }

        /*if(!notcompleted.isEmpty)
        {
          logger.info(s"\n${Console.RED_B}not completed: ${x}${Console.RESET}\n")
        }*/

        vertx.setTimer(10L, checkCompleted)

      case Failure(ex) => throw ex
    }

  }

  //vertx.setTimer(10L, checkCompleted)

  def handler(recs: Seq[CommittableMessage[String, Array[Byte]]]): Future[CommittableOffset] = {

    val dones = recs.map{rec => Any.parseFrom(rec.record.value()).unpack(BatchDone)}

    logger.info(s"${Console.YELLOW_B}$name RECEIVED dones: ${dones.map(_.id)} ${Console.RESET}\n")

    dones.foreach { done =>

      done.succeed.foreach { cid =>
        processing.remove(cid) match {
          case Some((c, p)) => p.success(TaskResponse(cid, true))
          case None =>
        }
      }

      done.failed.foreach { cid =>
        processing.remove(cid) match {
          case Some((c, p)) => p.success(TaskResponse(cid, false))
          case None =>
        }
      }

    }

    Future.successful(recs.sortBy(_.record.offset())).map(_.last.committableOffset)
  }

  val consumerSettings = ConsumerSettings[String, Array[Byte]](system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId(s"$name-consumer")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    .withClientId(s"$name-client")
    .withPollInterval(java.time.Duration.ofMillis(10L))
    .withStopTimeout(java.time.Duration.ofHours(1))
  //.withStopTimeout(java.time.Duration.ofSeconds(1000L))

  val committerSettings = CommitterSettings(system).withDelivery(CommitDelivery.waitForAck)

  val control =
    Consumer
      .committableSource(consumerSettings,
        Subscriptions.assignment(new TopicPartition(Topics.COORDINATORS, id)))
      .grouped(100)
      //.groupedWithin(10, 1 second)
      //.batch(100, )
      .mapAsync(1){ list =>
        handler(list)
      }
      .log("debugging")
      .via(Committer.batchFlow(committerSettings.withMaxBatch(100)))
      /*.toMat(Sink.ignore)(DrainingControl.apply)
      .run().streamCompletion*/
      .runWith(Sink.ignore)
      .recover {
        case e: RuntimeException => logger.error(s"\n${e.getMessage}\n")
      }

  override def submit(cmd: Command): Future[TaskResponse] = {
    logger.info(s"${Console.GREEN_B}RECEIVED REQUEST: ${cmd.id}${Console.RESET}")

    val pr = Promise[TaskResponse]()
    queue.put(cmd.id, cmd -> pr)

    pr.future

    //Future.successful(TaskResponse(cmd.id, true))
  }

  def close(): Future[Void] = {
    session.closeFuture()
    vertx.close()
  }
}
