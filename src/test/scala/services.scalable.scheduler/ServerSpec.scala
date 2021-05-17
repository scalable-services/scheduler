package services.scalable.scheduler

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import services.scalable.scheduler.server.CoordinatorServer

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ServerSpec extends AnyFlatSpec {

  "it " should "start coordinator servers successfully" in {

    // Create an Akka system
    val system = ActorSystem[Nothing](Behaviors.empty[Nothing], "Coordinators", ConfigFactory.load("server.conf"))

    implicit val ec = system.executionContext

    //Starting testing servers...
    for (i <- 0 until Config.NUM_COORDINATORS) {
       new CoordinatorServer(s"coord-$i", 3550 + i, i, system).run()
    }

    Await.ready(system.whenTerminated, Duration.Inf)

  }

}
