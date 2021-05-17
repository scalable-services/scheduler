package services.scalable

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import io.vertx.core.{Handler, VertxOptions}
import io.vertx.kafka.client.consumer.KafkaConsumer

import java.util.concurrent.{CompletionStage, TimeUnit}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.FutureConverters._
import scala.util.hashing.MurmurHash3

package object scheduler {

  implicit def vertxftosf[T](f: io.vertx.core.Future[T]): Future[T] = f.toCompletionStage.asScala
  implicit def jftsf[T](cs: CompletionStage[T]): Future[T] = cs.asScala

  implicit def jftsf[T](f: java.util.concurrent.Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val p = Promise[T]()

    ec.execute { case _ =>
      try {
        p.success(f.get())
      } catch {
        case ex: Throwable => p.failure(ex)
      }
    }

    p.future
  }

  implicit class CustomKakfaConsumer[K, V](consumer: KafkaConsumer[K, V]){
    def commitScala(): Future[Void] = consumer.commit()
  }

  val voptions = new VertxOptions()
    .setMaxWorkerExecuteTime(3L)
    .setMaxEventLoopExecuteTime(3L)
    .setMaxEventLoopExecuteTimeUnit(TimeUnit.SECONDS)
    .setMaxWorkerExecuteTimeUnit(TimeUnit.SECONDS)
    .setWorkerPoolSize(4)
    .setBlockedThreadCheckInterval(3L)
    .setBlockedThreadCheckIntervalUnit(TimeUnit.SECONDS)

  val loader =
    DriverConfigLoader.programmaticBuilder()
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(30))
      .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 31768)
      .withInt(DefaultDriverOption.SESSION_LEAK_THRESHOLD, 1000)
      .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
      .withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS, "ExponentialReconnectionPolicy")
      .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, java.time.Duration.ofSeconds(1))
      .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, java.time.Duration.ofSeconds(10))
      /*.startProfile("slow")
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
      .endProfile()*/
      .build()

  object Config {

    val KEYSPACE = "scheduler"

    val CASSANDRA_HOST = "localhost"
    val CASSANDRA_PORT = 9042
    val DC = "datacenter1"

    val TIMEOUT = 20

    val KAFKA_HOST = "localhost:9092"
    val ZOOKEEPER_HOST = "localhost:2181"

    val NUM_DATA_PARTITIONS = Int.MaxValue

    val NUM_META_PARTITIONS = 3

    val NUM_SCHEDULERS = 3
    val NUM_COORDINATORS = 3
    val NUM_WORKERS = 3

    //in ms
    val COORDINATOR_BATCH_INTERVAL = 10L

    val MAX_COORDINATOR_BUFFER_SIZE = 20

    val AGGREGATOR_INTERVAL = 100L

    val MAX_SCHEDULER_POLL_RECORDS = 10L
  }

  var topics = Map(
    Topics.COORDINATORS -> Config.NUM_COORDINATORS,
    Topics.LOG -> 1,
    //Topics.SCHEDULERS -> Config.NUM_SCHEDULERS,
    //Topics.VOTES -> Config.NUM_SCHEDULERS,
    Topics.HANDLERS -> 3,
    Topics.METAS -> Config.NUM_META_PARTITIONS,

    //Topics.DONE -> Config.NUM_SCHEDULERS
  )

  for(i<-0 until Config.NUM_SCHEDULERS){
    topics = topics + (s"${Topics.SCHEDULERS}-${i}" -> 1)
  }

  object Topics {
    val METAS = "metas"
    val LOG = "log"
    val COORDINATORS = "coordinators"
    val SCHEDULERS = "schedulers"
    val HANDLERS = "handlers"
    val VOTES = "votes"
  }

  def computeScheduler(id: String, length: Int): Int = {
    MurmurHash3.stringHash(id).abs % length
  }

  def computePartition(key: String): Int = {
    MurmurHash3.stringHash(key).abs % Config.NUM_DATA_PARTITIONS
  }

  def computeScheduler(k: String): Int = {
    MurmurHash3.stringHash(k).abs % Config.NUM_SCHEDULERS
  }

}
