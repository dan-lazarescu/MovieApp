package actor

import org.slf4j.LoggerFactory
import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import service.WebService
import model.SupervisorActor.{StartConsumer, StartWebserver}
import model.KafkaConsumerActor.StartConsuming

class SupervisorActor(implicit val cassandraSession: CassandraSession,
                      implicit val system: ActorSystem)
  extends Actor {

  private val logger = LoggerFactory.getLogger(classOf[SupervisorActor])
  private val actorDb = context.actorOf(model.MoviesRepo.props(cassandraSession), "DbActor")
  private val actorKafka = context.actorOf(model.KafkaConsumerActor.props(cassandraSession), "KafkaActor")
  context.watch(actorDb)
  context.watch(actorKafka)

  override def receive: Receive = {
    case StartConsumer =>
      logger.info("Starting consumer server...")
      actorKafka ! StartConsuming

    case StartWebserver =>
      val webServer = new WebService(actorDb)
      logger.info("Starting WebServer...")
      webServer.startServer()

    case _ => logger.info(s"Received something from ${sender()}")
  }

}
