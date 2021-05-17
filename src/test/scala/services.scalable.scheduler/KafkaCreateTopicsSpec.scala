package services.scalable.scheduler

import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class KafkaCreateTopicsSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  "it " should " delete topics successfully" in {

    Await.result(KafkaAdminHelper.create(topics).map { _ =>
      logger.info(s"${Console.BLUE_B}created topics: ${topics}...${Console.RESET}\n\n")
    }, Duration.Inf)

  }

}
