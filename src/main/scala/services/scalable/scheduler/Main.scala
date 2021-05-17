package services.scalable.scheduler

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior, SupervisorStrategy}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityTypeKey, ShardedDaemonProcess}
import akka.cluster.singleton.ClusterSingletonManager
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import akka.util.Timeout
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Main {

  object RootBehavior {
    def apply(port: Int): Behavior[Nothing] = Behaviors.setup[Nothing] { context =>
      // Create an actor that handles cluster domain events
      //context.spawn(ClusterListener(), "ClusterListener")

      //val sharding = ClusterSharding(context.system)
      val system = context.system
      implicit val scheduler = context.system.scheduler

      /*val ref = sharding.init(
        Entity(TypeKey)(createBehavior = entityContext =>
          Greeter(entityContext.entityId, entityContext))
      )*/

      val daemon =  ShardedDaemonProcess(system)

      val SCHEDULERS = (0 until Config.NUM_SCHEDULERS).map { i =>
        s"scheduler-${i}"
      }

      daemon.init("schedulers", SCHEDULERS.length, id => Behaviors.supervise(Scheduler(SCHEDULERS(id), id))
        .onFailure[Exception](SupervisorStrategy.restart), Scheduler.Stop)

      val singletonManager = ClusterSingleton(system)

      singletonManager.init(
        SingletonActor(Behaviors.supervise(Aggregator()).onFailure[Exception](SupervisorStrategy.restart), "Aggregator"))

      Behaviors.empty
    }
  }

  def main(args: Array[String]): Unit = {

    val ports =
      if (args.isEmpty)
        Seq(2551, 2552, 2553)
      else
        args.toSeq.map(_.toInt)

    ports.foreach(startup)
  }

  def startup(port: Int): Unit = {

    // Override the configuration of the port
    val config = ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port=$port
      """).withFallback(ConfigFactory.load())

    // Create an Akka system
    ActorSystem[Nothing](RootBehavior(port), "Scheduler", config)
  }

}