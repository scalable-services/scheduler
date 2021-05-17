package services.scalable.scheduler.server

//#import

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.Http
import services.scalable.scheduler.protocol._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CoordinatorServer(val name: String, val port: Int, val id: Int, val system: ActorSystem[Nothing]) {

  def run(): Future[Http.ServerBinding] = {
    implicit val sys = system
    implicit val ec: ExecutionContext = system.executionContext

    val coordinator = new Coordinator(name, id, port)

    val bind = Http().newServerAt("127.0.0.1", port)
      //.withSettings(ServerSettings.apply(sys).withBacklog(1000))
      .bind(CoordinatorServiceHandler(coordinator))
      .map(_.addToCoordinatedShutdown(hardTerminationDeadline = 5.minutes))

      sys.whenTerminated.flatMap(_ => coordinator.close())

      bind.onComplete {
        case Success(binding) =>
          val address = binding.localAddress
          println(s"${Console.MAGENTA_B}gRPC server bound to {}:{}${Console.RESET}", address.getHostString, address.getPort)
        case Failure(ex) =>
          println("Failed to bind gRPC endpoint, terminating system", ex)
          system.terminate()
      }

    bind
  }

}
