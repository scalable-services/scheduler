package services.scalable.scheduler

import io.vertx.core.Vertx
import io.vertx.kafka.admin.{KafkaAdminClient, NewTopic}
import io.vertx.kafka.client.common.TopicPartition
import io.vertx.kafka.client.consumer.KafkaConsumer
import org.apache.kafka.clients.admin.AdminClientConfig

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

object KafkaAdminHelper {

  val config = new java.util.Properties()
  config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG , Config.KAFKA_HOST)

  val vertx = Vertx.vertx(voptions)

  val admin = KafkaAdminClient.create(vertx, config)

  def create(topics: Map[String, Int])(implicit ec: ExecutionContext): Future[Boolean] = {
    admin.createTopics(topics.map{case (name, p) => new NewTopic(name, p, 1.toShort)}.toSeq.asJava).toCompletionStage.asScala.map(_ => true)
  }

  def delete(topics: Seq[String])(implicit ec: ExecutionContext): Future[Boolean] = {
    admin.deleteTopics(topics.asJava).toCompletionStage.asScala.map(_ => true)
  }

  def topicsExists(topics: Seq[String])(implicit ec: ExecutionContext): Future[Seq[String]] = {
    admin.listTopics().toCompletionStage.asScala.map(_.asScala.toSeq).map{list => list.filter(s => list.contains(s))}
  }

  def configure(partition: TopicPartition, consumer: KafkaConsumer[String, Array[Byte]], offset: Long)(implicit ec: ExecutionContext): Future[Boolean] = {
    /*consumer.assignFuture(partition)
      .flatMap(_ => consumer.endOffsetsFuture(partition))
      .flatMap(last => consumer.seekFuture(partition, last))
      /*.flatMap(_ => YugabyteAdminHelper.insertOffsets(partition.getTopic, partition.getPartition, name))
      .flatMap(_ => YugabyteAdminHelper.getOffset(partition.getTopic, partition.getPartition, name))
      .flatMap(offset => consumer.seekFuture(partition, offset))*/
      .map(_ => true)*/

    consumer.assign(partition).toCompletionStage.asScala
      .flatMap(_ => consumer.seek(partition, offset))
      .map(_ => true)
  }

}