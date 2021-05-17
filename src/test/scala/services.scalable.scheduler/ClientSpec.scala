package services.scalable.scheduler

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.grpc.GrpcClientSettings
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.scheduler.protocol._

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class ClientSpec extends AnyFlatSpec with Repeatable {

  override val times = 100

  val logger = LoggerFactory.getLogger(this.getClass)

  "it" should " keep the sum of money equal before and after test" in {

    val rand = ThreadLocalRandom.current()

    implicit val sys = ActorSystem(Behaviors.empty[Any], "Scheduler", ConfigFactory.load("client.conf"))
    implicit val ec: ExecutionContext = sys.executionContext

    /*val client = CoordinatorServiceClient(GrpcClientSettings.fromConfig("scheduler.CoordinatorService")
      .withTls(false))*/

    var clients = Seq.empty[CoordinatorServiceClient]

    for(i<-0 until 10){
      val port = 3550 + rand.nextInt(0, 2)

      val client = CoordinatorServiceClient(
        GrpcClientSettings
          .connectToServiceAt("localhost", port)
          .withTls(false)
      )

      clients = clients :+ client
    }

    val session = CqlSession
      .builder()
      //.addContactPoint(new InetSocketAddress("localhost", 9042))
      .withConfigLoader(loader)
      //.withLocalDatacenter(Config.DC)
      .withKeyspace(Config.KEYSPACE)
      .build()

    var accounts = Seq.empty[String]

    val results = session.execute(s"select * from data;")

    results.forEach(r => {
      val key = r.getString("id")
      val value = r.getInt("value")

      accounts = accounts :+ key
    })

    val bankBefore = session.execute("select sum(value) as total from data;").one.getInt("total")

    val READ_DATA  = session.prepare(s"select value, version from data where id=?;")

    def readData(k: String): Future[(String, Int)] = {
      session.executeAsync(READ_DATA.bind().setString(0, k)).map { rs =>
        val one = rs.one()
        one.getString("version") -> one.getInt("value")
      }
    }

    def submit(): Future[TaskResponse] = {
      val client = clients(rand.nextInt(0, clients.length))

      val tid = UUID.randomUUID.toString()
      val keys = (0 until 2).map { _ => accounts(rand.nextInt(0, accounts.length)) }

      val k0 = keys(0)
      val k1 = keys(1)

      if(k0.equals(k1)) return Future.successful(TaskResponse(tid, true))

      val t0 = System.currentTimeMillis()

      Future.sequence(keys.map { k => readData(k).map(k -> _) }).flatMap { values =>

        val (k0, (vs0, v0)) = values(0)
        val (k1, (vs1, v1)) = values(1)

        var reads = Seq.empty[(String, String)]
        var writes = Seq.empty[(String, Int)]

        val ammount = if (v0 > 1) rand.nextInt(0, v0) else v0

        val w0 = v0 - ammount
        val w1 = v1 + ammount

        reads = reads ++ Seq(
          k0 -> vs0,
          k1 -> vs1
        )

        writes = writes ++ Seq(
          k0 -> w0,
          k1 -> w1
        )

        val cmd = Command(
          tid,
          reads.toMap,
          writes.toMap
        ).withSchedulers(Seq(k0, k1).map{k => computeScheduler(k)}.distinct)

        //logger.info(s"${Console.BLUE_B}COMMAND ${tid} => ${reads}${Console.RESET}")

        client.submit(cmd).map { tr =>
          val elapsed = System.currentTimeMillis() - t0

          logger.info(s"${if(tr.succeed) Console.GREEN_B else Console.RED_B}received ${tr.id} in ${elapsed} ms${Console.RESET}")

          tr.withElapsed(elapsed)
        }
      }
    }

    var tasks = Seq.empty[Future[TaskResponse]]

    for(i<-0 until 1000){
      tasks = tasks :+ submit()
    }

    val response = Await.result(Future.sequence(tasks), Duration.Inf)

    val bankAfter = session.execute("select sum(value) as total from data;").one.getInt("total")

    logger.info(s"${Console.GREEN_B}RECEIVED RESPONSES ${response.map(_.id)}${Console.RESET}")

    var elapsed = 0L

    val len = response.length.toFloat
    val successes = response.count(_.succeed == true)

    response.foreach{tr => elapsed += tr.elapsed}

    logger.info(s"${Console.YELLOW}total before ${bankBefore} total after ${bankAfter}${Console.RESET}\n\n")
    logger.info(s"${Console.MAGENTA_B}success rate: ${(successes/len) * 100} => ${successes}/${len} avg exec time: ${elapsed/1000.0}ms${Console.RESET}")

    assert(bankBefore == bankAfter)

    session.close()
    clients.foreach{_.close()}
    sys.terminate()

  }

}
