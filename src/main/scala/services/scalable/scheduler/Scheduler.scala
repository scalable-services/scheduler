package services.scalable.scheduler

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{Behavior, PostStop, PreRestart}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.kafka._
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.oss.driver.api.core.{CqlSession, DefaultConsistencyLevel}
import com.google.protobuf.any.Any
import com.typesafe.config.ConfigFactory
import io.vertx.core.Vertx
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import services.scalable.scheduler.protocol._

import scala.jdk.FutureConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.language.postfixOps
object Scheduler {

  trait Command extends CborSerializable
  final case object Stop extends Command

  final case class Process(pm: PartitionMeta) extends Command

  def apply(name: String, id: Int): Behavior[Command] = Behaviors.setup[Command] { ctx =>

    val logger = ctx.log
    implicit val ec = ctx.executionContext

    val system = ctx.system

    val streamsSystem = ActorSystem.create("StreamsSystem", ConfigFactory.empty())
    implicit val provider = streamsSystem.classicSystem

    /*val streamsSystem = system
    implicit val provider = streamsSystem.classicSystem*/

    val vertx = Vertx.vertx(voptions)

    val session = CqlSession
      .builder()
      //.addContactPoint(new InetSocketAddress(Config.CASSANDRA_HOST, Config.CASSANDRA_PORT))
      .withConfigLoader(loader)
      //.withLocalDatacenter(Config.DC)
      .withKeyspace(Config.KEYSPACE)
      .build()

    var pm: PartitionMeta = null
    val processing = TrieMap.empty[String, MetaBatch]

    val producerSettings = ProducerSettings(streamsSystem, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers("localhost:9092")

    val kafkaProducer = producerSettings.createKafkaProducer()
    val settingsWithProducer = producerSettings.withProducer(kafkaProducer)

    val READ_DATA = session.prepare(s"select value, version from data where id=?;")
    val UPDATE_DATA = session.prepare(s"update data set value=?, version=? where id=?;")

    val READ_COMMANDS = session.prepare(s"select * from commands where batch_id=? and completed=false;")
    val COMPLETE_COMMAND = session.prepare(s"update commands set completed=true, succeed=? where command_id=?;")

    val INSERT_META_BATCH = session.prepare("insert into batches(id, completed, votes) values(?, false, {}) IF NOT EXISTS;")

    val READ_SCHEDULER_COMMANDS = session.prepare(s"select * from scheduler_commands where batch_id=? and scheduler=?;")

    val READ_EPOCH_RESULTS = session.prepare(s"select successes, failures from batches where id=?;")
    val INCREMENT_EPOCH_COMPLETED = session.prepare(s"update batches set writes_completed = writes_completed + 1 where id=?;")

    val UPDATE_CONSUMER_OFFSET = session.prepare(s"UPDATE log_offsets SET offset=? WHERE consumer=?;")
    val READ_CONSUMER_OFFSET = session.prepare(s"SELECT offset FROM log_offsets WHERE consumer=?;")

    def updateLogOffset(offset: Long): Future[Boolean] = {
      session.executeAsync(UPDATE_CONSUMER_OFFSET.bind().setLong(0, offset).setString(1, name)).map(_.wasApplied())
    }

    def getLastLogOffset(): Future[Long] = {
      session.executeAsync(READ_CONSUMER_OFFSET.bind().setString(0, name)).map{ rs =>
        val one = rs.one()
        if(one != null) one.getLong("offset") else 0L
      }
    }

    def readData(k: String): Future[(String, Int)] = {
      session.executeAsync(READ_DATA.bind().setString(0, k)).map { rs =>
        val one = rs.one()
        one.getString("version") -> one.getInt("value")
      }
    }

    def writeData(k: String, v: Int, vs: String): Future[Boolean] = {
      session.executeAsync(UPDATE_DATA.bind().setInt(0, v).setString(1, vs).setString(2, k)
        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).map(_.wasApplied())
    }

    def completeCommand(c: protocol.Command, succeed: Boolean): Future[Boolean] = {
      session.executeAsync(COMPLETE_COMMAND.bind().setBoolean(0, succeed).setString(1, c.id)
        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).map(_.wasApplied())
    }

    val COMPLETE_META_BATCH = session.prepare("update batches set completed = true where id=?;")
    val UPDATE_META_BATCH = session.prepare("update batches set completed = true, successes = successes + ?, failures = failures + ? where id=?;")
    val CHECK_META_BATCH = session.prepare("select * from batches where id=?;")
    val VOTE_BATCH = session.prepare(s"update batches set votes = votes + ?, successes = successes + ?, failures = failures + ? where id=? if exists")

    val CHECK_VOTES = session.prepare(s"select votes from batches where id=?;")

    val UPDATE_BATCH = session.prepare(s"update batches set workers = workers + ? where id=?;")

    def completeMeta(b: MetaBatch): Future[Boolean] = {
      session.executeAsync(COMPLETE_META_BATCH.bind().setString(0, b.id)
        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).map(_.wasApplied())
    }

    def completeMeta3(b: MetaBatch, succeed: Seq[String], failed: Seq[String]): Future[Boolean] = {
      session.executeAsync(UPDATE_META_BATCH.bind()
        .setSet(0, succeed.toSet.asJava, classOf[String])
        .setSet(1, failed.toSet.asJava, classOf[String])
        .setString(2, b.id)
        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).map(_.wasApplied())
    }

    def completeMeta2(e: Epoch): Future[Boolean] = {
      session.executeAsync(COMPLETE_META_BATCH.bind().setString(0, e.id)
        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).map(_.wasApplied())
    }

    def checkMetabatch(pm: MetaBatch): Future[Boolean] = {
      //session.executeAsync(CHECK_META_BATCH.bind().setString(0, pm.id)).map {_.one() != null}

      session.executeAsync(CHECK_META_BATCH.bind().setString(0, pm.id)).map { rs =>
        val one = rs.one()
        if(one == null) false else one.getBoolean("completed")
      }
    }

    def checkEpochCompleted(e: Epoch): Future[Boolean] = {
      session.executeAsync(CHECK_META_BATCH.bind().setString(0, e.id)).map { rs =>
        val one = rs.one()
        one.getInt("writes_completed") == e.schedulers.length
      }
    }

    def updateMetaInfo(pm: MetaBatch): Future[Boolean] = {
      session.executeAsync(UPDATE_BATCH.bind().setSet(0, pm.schedulers.map(_.toString).toSet.asJava, classOf[String])
        .setString(1, pm.id).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)).map(_.wasApplied())
    }

    def getMetabachInfo(pm: MetaBatch): Future[(Boolean, Seq[Int])] = {
      session.executeAsync(CHECK_META_BATCH.bind().setString(0, pm.id)).map { rs =>
        val one = rs.one()
        val schedulers = one.getSet[String]("workers", classOf[String]).asScala.map(_.toInt).toSeq
        val completed = one.getBoolean("completed")

        completed -> schedulers
      }
    }

    def voteBatch(e: MetaBatch, successes: Set[String] = Set.empty[String], failures: Set[String] = Set.empty[String]): Future[Boolean] = {
      session.executeAsync(VOTE_BATCH.bind()
        .setSet(0, Set(id.toString).asJava, classOf[String])
        .setSet(1, successes.asJava, classOf[String])
        .setSet(2, failures.asJava, classOf[String])
        .setString(3, e.id)
        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
      ).map(_.wasApplied())
    }

    def voteBatch3(e: Epoch, successes: Set[String] = Set.empty[String], failures: Set[String] = Set.empty[String]): Future[Boolean] = {
      session.executeAsync(VOTE_BATCH.bind()
        .setSet(0, Set(id.toString).asJava, classOf[String])
        .setSet(1, successes.asJava, classOf[String])
        .setSet(2, failures.asJava, classOf[String])
        .setString(3, e.id)
        .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_QUORUM)
      ).map(_.wasApplied())
    }

    def checkVotes(e: MetaBatch): Future[Boolean] = {
      session.executeAsync(CHECK_VOTES.bind().setString(0, e.id)).map { rs =>
        val one = rs.one()

        if(one == null) {
          false
        } else {
          val votes = one.getSet[String]("votes", classOf[String]).asScala.map(_.toInt)
          e.schedulers.filterNot{_ == id}.forall{votes.contains(_)}
        }
      }
    }

    def checkVotes2(e: MetaBatch, schedulers: Seq[Int]): Future[Boolean] = {
      session.executeAsync(CHECK_VOTES.bind().setString(0, e.id)).map { rs =>
        val one = rs.one()

        if(one == null) {
          false
        } else {
          val votes = one.getSet[String]("votes", classOf[String]).asScala.map(_.toInt)
          schedulers.forall{votes.contains(_)}
        }
      }
    }

    def checkVotes3(e: Epoch, schedulers: Seq[Int]): Future[Boolean] = {
      session.executeAsync(CHECK_VOTES.bind().setString(0, e.id)).map { rs =>
        val one = rs.one()

        if(one == null) {
          false
        } else {
          val votes = one.getSet[String]("votes", classOf[String]).asScala.map(_.toInt)
          schedulers.forall{votes.contains(_)}
        }
      }
    }

    def getCommands(bid: String): Future[Seq[(protocol.Command, Option[Boolean])]] = {
      session.executeAsync(READ_COMMANDS.bind().setString(0, bid)).flatMap { rs =>
        var commands = Seq.empty[(protocol.Command, Option[Boolean])]

        def fetchPage(): Future[Seq[(protocol.Command,  Option[Boolean])]] = {
          val page = rs.currentPage()

          page.forEach { r =>
            val completed = if(r.getBoolean("completed")) Some(r.getBoolean("succeed")) else None
            commands = commands :+ (Any.parseFrom(r.getByteBuffer("data").array()).unpack(protocol.Command), completed)
          }

          if (rs.hasMorePages) {
            return rs.fetchNextPage().flatMap(_ => fetchPage())
          }

          Future.successful(commands)
        }

        fetchPage()
      }
    }

    def executec(c: protocol.Command): Future[Boolean] = {

      def writes(): Future[Boolean] = {
        Future.sequence(c.writes.map { case (k, v) => writeData(k, v, c.id) }).flatMap { results =>
          if (results.exists(_ == false)) {
            Future.successful(false)
          } else {
            completeCommand(c, true)
          }
        }
      }

      def checkChanges(versions: Seq[(String, Boolean)]): Future[Boolean] = {
        if (versions.exists(_._2 == false)) {
          return completeCommand(c, false)
        }

        // logger.info(s"${Console.CYAN_B}writing ${c.writes}${Console.RESET}")

        writes()
      }

      Future.sequence(c.reads.map { case (k, vs) => readData(k).map { case (rvs, _) => k -> (vs.equals(rvs) || vs.equals(c.id)) } })
        .flatMap { versions => checkChanges(versions.toSeq) }
    }

    def calculateGroups(result: Seq[Seq[MetaBatch]], metas: Seq[MetaBatch]): Seq[Seq[MetaBatch]] = {
      if(metas.isEmpty){
        return result
      }

      var partitions = Seq.empty[Int]
      var list = Seq.empty[MetaBatch]
      var others = Seq.empty[MetaBatch]

      metas.foreach { m =>
        if(!m.partitions.exists{partitions.contains(_)}){
          list = list :+ m
          partitions = partitions ++ m.partitions
        } else {
          others = others :+ m
        }
      }

      calculateGroups(result :+ list, others.sortBy(_.id))
    }

    def notifyCoordinators(list: Seq[BatchDone]): Future[Boolean] = {
      if(list.isEmpty) return Future.successful(true)

      val now = System.currentTimeMillis()

      val records = list.map { done =>
        val buf = Any.pack(done).toByteArray

        new ProducerRecord(Topics.COORDINATORS, done.coordinator, now, done.id, buf)
      }

      Source(records)
        .runWith(Producer.plainSink(settingsWithProducer)).map(_ => true)
    }

    def process(all: Seq[MetaBatch]): Future[Boolean] = {

      val local = all.filter{_.schedulers.contains(id)}

      if(local.isEmpty){
        return Future.successful(true)
      }

      val pr = Promise[Boolean]()

      logger.info(s"${Console.CYAN_B}$name processing pm ${pm.id} all batches: ${all.map(_.id)}${Console.RESET}")

      val groups = calculateGroups(Seq.empty[Seq[MetaBatch]], all)

      def next(idx: Int = 0): Unit = {

        if(idx == groups.length) {
          pr.success(true)
          return
        }

        val list = groups(idx)
        val head = list.head

        val local = list.filter{_.schedulers.contains(id)}

        if(local.isEmpty){
          next(idx + 1)
          return
        }

        val schedulers = local.map(_.schedulers).flatten.distinct

        def exec(): Unit = {

          val list = local.filter{_.scheduler == id}

          if(list.isEmpty){
            next(idx + 1)
            return
          }

          list.foreach {m => processing.put(m.id, m)}

          /*val promise = Promise[Boolean]()

          def check(): Unit = {

            if(processing.isEmpty){
              promise.success(true)
              return
            }

            vertx.setTimer(10L, _ => check())
          }

          postJobs(list).flatMap{_ =>
            vertx.setTimer(10L, _ => check())
            promise.future
          }.onComplete {
            case Success(ok) => next(idx + 1)
            case Failure(ex) => throw ex
          }*/

          Future.sequence(list.map(_.commands).flatten.map{c => executec(c).map{c -> _}}).onComplete {
            case Success(results) =>
              val succeed = results.filter(_._2).map(_._1.id)
              val failed = results.filter(!_._2).map(_._1.id)

              Future.sequence(list.map{m => completeMeta3(m, succeed, failed).map{m -> _}}).flatMap { _ =>
                notifyCoordinators(list.map { m =>
                  BatchDone(m.id, m.coordinator, m.commands.filter{c => succeed.contains(c.id)}.map(_.id),
                    m.commands.filter{c => failed.contains(c.id)}.map(_.id), id)
                })
              }.onComplete {
                case Success(results) => next(idx + 1)
                case Failure(ex) => pr.failure(ex)
              }

            case Failure(ex) => pr.failure(ex)
          }
        }

        def ready(): Unit = {
          checkVotes2(head, schedulers).onComplete {
            case Success(ok) => if(ok) exec() else vertx.setTimer(10L, _ => ready())
            case Failure(ex) => pr.failure(ex)
          }
        }

        voteBatch(head).onComplete {
          case Success(ok) => ready()
          case Failure(ex) => pr.failure(ex)
        }
      }

      next()

      pr.future
    }

    val metaSettings = ConsumerSettings[String, Array[Byte]](streamsSystem, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers("localhost:9092")
      .withClientId(s"$name-meta-app")

    /*Consumer.plainPartitionedManualOffsetSource(metaSettings, Subscriptions.topics("demo"), partitions => {

      Future.successful(Map.empty[TopicPartition, Long])
    })*/

    def poll(p: TopicPartition, start: Long, n: Int): Future[Seq[MetaBatch]] = {
      Consumer.plainSource(metaSettings, Subscriptions.assignmentWithOffset(new TopicPartition(p.topic(), p.partition()) -> start))
        .mapAsync(1){ msg =>
          Future.successful(Any.parseFrom(msg.value()).unpack(MetaBatch))
        }
        .take(n)
        .runWith(Sink.seq)
    }

    def getRange(): Future[Boolean] = {

      val p = new TopicPartition(pm.topic, pm.partition)

      logger.info(s"${Console.BLUE_B}$name RANGE FOR PM ${pm.id} : ${pm.offset - pm.last}${Console.RESET}")

      def poll1(start: Long, end: Long): Future[Boolean] = {

        if(start == end) return Future.successful(true)

        val n = Math.min(end - start, Config.MAX_SCHEDULER_POLL_RECORDS).toInt

        poll(p, start, n).flatMap { all =>
          process(all).flatMap { _ =>
            poll1(start + n, pm.offset)
          }
        }
      }

      poll1(pm.last, pm.offset)
    }

    val consumerSettings = ConsumerSettings[String, Array[Byte]](streamsSystem, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId(s"$name-consumer")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      .withClientId(s"$name-client")
      .withPollInterval(java.time.Duration.ofMillis(5L))
      .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "10")
      .withStopTimeout(java.time.Duration.ofDays(1))
      .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")
    //.withStopTimeout(java.time.Duration.ofSeconds(1000L))

    val committerSettings = CommitterSettings(streamsSystem).withDelivery(CommitDelivery.waitForAck)

    var promise: Promise[Boolean] = null

    val control = Consumer
        .committableSource(consumerSettings, Subscriptions.topics(Topics.LOG))
        //.groupedWithin(Config.MAX_SCHEDULER_POLL_RECORDS, 10 milliseconds)
        .mapAsync(1) { msg =>
          //handler(msg).map(_ => msg.committableOffset)

          ctx.self ! Process(Any.parseFrom(msg.record.value()).unpack(PartitionMeta))

          promise = Promise[Boolean]()
          promise.future.map(_ => msg.committableOffset)

          /*pm = Any.parseFrom(msg.record.value()).unpack(PartitionMeta)
          getRange().map(_ => msg.committableOffset)*/

        }
        .log("debugging")
        .via(Committer.flow(committerSettings.withMaxBatch(1)))
        //.runWith(Sink.ignore)
        .recover {
          case e: RuntimeException => logger.error(s"\n${e.getMessage}\n")
        }
        .toMat(Sink.ignore)(DrainingControl.apply).run()

    logger.info(s"\n${Console.GREEN_B}$name STARTING...${Console.RESET}\n")

    def closeAll(): Behavior[Command] = {

      session.close()

      val f = for {
        _ <- vertx.close().toCompletionStage.asScala
        _ <- control.stop()
        _ <- streamsSystem.terminate()
      } yield {}

      Await.result(f, Duration.Inf)

      Behaviors.same
    }

    def handlePM(m: PartitionMeta): Behavior[Command] = {
      pm = m

      getRange().onComplete {
        case Success(ok) => promise.success(ok)
        case Failure(ex) => promise.failure(ex)
      }

      Behaviors.same
    }

    Behaviors.receiveMessage[Command] {
      case Scheduler.Process(m) => handlePM(m)
      case Scheduler.Stop => Behaviors.stopped
      case _ => Behaviors.same
    }.receiveSignal {
      case (context, PreRestart) => Behaviors.same
      case (context, PostStop) =>

        logger.info(s"\n${Console.RED_B}$name STOPPED...${Console.RESET}\n")

        closeAll()
    }
  }

}
