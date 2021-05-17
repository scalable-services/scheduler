package services.scalable.scheduler

import com.datastax.oss.driver.api.core.CqlSession
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

class InsertSpec extends AnyFlatSpec {

  "insertion" should "execute succesfully" in {

    val session = CqlSession
      .builder()
      .withConfigLoader(loader)
      .withKeyspace(Config.KEYSPACE)
      .build()

    session.execute("truncate table data;")

    val INSERT_DATA = session.prepare("insert into data(id, value, version) values(?,?,?);")

    val n = 5000
    val rand = ThreadLocalRandom.current()
    val MAX_VALUE = 1000
    val tid = UUID.randomUUID.toString

    for(i<-0 until n){
      val key = UUID.randomUUID.toString
      session.execute(INSERT_DATA.bind().setString(0, key).setInt(1, rand.nextInt(0, MAX_VALUE)).setString(2, tid))
    }

    session.close()
  }

}