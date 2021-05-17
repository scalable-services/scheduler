package services.scalable.scheduler

import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Main {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    new Aggregator()

    for(i<-0 until Config.NUM_SCHEDULERS){
      val scheduler = new Scheduler(s"scheduler-$i", i)
    }

  }

}
