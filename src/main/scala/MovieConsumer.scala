import org.slf4j.LoggerFactory
import akka.actor.ActorSystem
import akka.stream.alpakka.cassandra.CassandraSessionSettings
import akka.stream.alpakka.cassandra.scaladsl._
import model.SupervisorActor._

import scala.concurrent.ExecutionContext

/**
TODO: 1. Initiate actor system with dependencies
2. start actor system to consume from kafka
3. init & start web server from a separated service object
**/

trait InitSetup {
  implicit val system = ActorSystem()
  implicit val ec = ExecutionContext.global
  val logger = LoggerFactory.getLogger("MovieConsumer")

  //Cassandra SetUp
  private val sessionSettings: CassandraSessionSettings = CassandraSessionSettings()
  val cassandraSession: CassandraSession =
    CassandraSessionRegistry.get(system).sessionFor(sessionSettings)
}

object MovieConsumer extends App with InitSetup {
  val supervisorActor = system.actorOf(model.SupervisorActor.props(system, cassandraSession), "SupervisorActor")
  logger.info("Starting App")
  supervisorActor ! StartConsumer
  supervisorActor ! StartWebserver

}
