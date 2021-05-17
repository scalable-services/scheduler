package services.scalable.scheduler

import com.datastax.oss.driver.api.core.CqlSession
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import java.net.InetSocketAddress

class ResetTopicsMetaSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  "it " should " delete topics successfully" in {

    val session = CqlSession
      .builder()
      //.addContactPoint(new InetSocketAddress(Config.CASSANDRA_HOST, Config.CASSANDRA_PORT))
      .withConfigLoader(loader)
      //.withLocalDatacenter(Config.DC)
      .withKeyspace(Config.KEYSPACE)
      .build()

    val truncates = Seq(session.execute(
      s"TRUNCATE TABLE scheduler_commands;").wasApplied(),
      session.execute(s"TRUNCATE TABLE batches;").wasApplied(),
      session.execute(s"truncate table partition_meta;").wasApplied()
    )

    logger.info(s"truncates results: ${truncates}")

  }

}
