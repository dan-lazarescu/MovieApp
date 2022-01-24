package actor

import scala.jdk.CollectionConverters._
import org.slf4j.LoggerFactory
import scala.concurrent.Future
import akka.actor.Actor
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.{ClosedShape, FanOutShape2, FlowShape, IOResult, Materializer, SourceShape}
import akka.stream.alpakka.cassandra.{CassandraSessionSettings, CassandraWriteSettings}
import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSession}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Keep, MergePreferred, RunnableGraph, Sink, Source, Zip}
import akka.util.ByteString
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import com.datastax.oss.driver.api.core.data.UdtValue
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroDeserializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import java.util.UUID.randomUUID
import java.nio.file.Paths

import model.{ExistingMovie, Movie}
import model.KafkaConsumerActor.StartConsuming

class KafkaConsumerActor(implicit val cassandraSession: CassandraSession) extends Actor {
  import utilities.Validations._
  import repo.MoviesRepo.{statementBinder, insertVideos}

  implicit val system = context.system
  implicit val dispatcher = system.dispatcher
  private val logger = LoggerFactory.getLogger(classOf[KafkaConsumerActor])

  override def receive: Receive = {
    case StartConsuming =>
      logger.info("starting flow")
      moviesSourceGraph.runForeach(println(_))
    case _ =>
      logger.info("No support for message")
      sender() ! "unsuported message"
  }

  //kafka settings
  private def genericConsumerSettings(broker: String, schemaRegistry: String, groupId: String): ConsumerSettings[String, GenericRecord] = {
    val kafkaAvroSerDeConfig = Map[String, Any] {
      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistry
    }
    val kafkaAvroDeserializer = new KafkaAvroDeserializer()
    kafkaAvroDeserializer.configure(kafkaAvroSerDeConfig.asJava, false)
    val deserializer = kafkaAvroDeserializer.asInstanceOf[Deserializer[GenericRecord]]

    ConsumerSettings(system, new StringDeserializer, deserializer)
      .withBootstrapServers(broker)
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  }
  private val consumerSettings =
    genericConsumerSettings("localhost:9092", "http://localhost:8081", randomUUID().toString())

  //rejection files
  private val rejectionFile = Paths.get("src/main/resources/rejected_records.txt")
  private val rejectionMovie = Paths.get("src/main/resources/rejected_movies.txt")

  //Sources and Sinks
  private val kafkaSource: Source[ConsumerRecord[String, GenericRecord], Consumer.Control] =
    Consumer.plainSource(consumerSettings, Subscriptions.topics("movies"))
  private val genericRecordSource = kafkaSource.map(_.value())
  private val rejectedMovieSink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(rejectionMovie)
  private val rejectedRecordsSink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(rejectionFile)

  //Shapes
  private val genericRecordsValidator = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val broadcastGeneric = builder.add(Broadcast[GenericRecord](2))
    val validRecords = builder.add(Flow[GenericRecord].filter(record => isValidRecord(record)))
    val rejectedRecords = builder.add(Flow[GenericRecord].filter(record => !isValidRecord(record)))
    val toByteString = builder.add(Flow[GenericRecord].map[ByteString](genRecord => ByteString(genRecord.toString)))

    broadcastGeneric.out(0) ~> validRecords
    broadcastGeneric.out(1) ~> rejectedRecords ~> toByteString

    new FanOutShape2(broadcastGeneric.in, validRecords.out, toByteString.out)
  }

  private val movieValidator = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val toMovieShape = builder.add(Flow[GenericRecord].map(toMovie))
    val broadcastMovie = builder.add(Broadcast[(GenericRecord, Movie)](2))
    val validMovie = builder.add(Flow[(GenericRecord, Movie)].filterNot(_._2.isEmpty).map(_._2))
    val existingMovie = builder.add(Flow[Movie].map(movie => movie.asInstanceOf[ExistingMovie]))
    val rejectedMovie = builder.add(Flow[(GenericRecord, Movie)].filter(_._2.isEmpty).map(_._1))
    val toByteString = builder.add(Flow[GenericRecord].map[ByteString](genRecord => ByteString(genRecord.toString)))

    toMovieShape.out ~> broadcastMovie
    broadcastMovie.out(0) ~> validMovie ~> existingMovie
    broadcastMovie.out(1) ~> rejectedMovie ~> toByteString

    new FanOutShape2(toMovieShape.in, existingMovie.out, toByteString.out)
  }

  private val insertMovie = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val udtValFuture = cassandraSession.underlying().map { session =>
      session.getMetadata
        .getKeyspace("moovie")
        .get()
        .getUserDefinedType("video_encoding")
        .get()
        .newValue()
    }
    val udtValSource = Source.future(udtValFuture)
    val mergePreferred = builder.add(MergePreferred[UdtValue](1))
    val zip = builder.add(Zip[ExistingMovie, UdtValue]())
    val broadcast = builder.add(Broadcast[(ExistingMovie, UdtValue)](2))
    val extractUdtVal = builder.add(Flow[(ExistingMovie, UdtValue)].map{ case (_, udtvalue) => udtvalue})

    val insertVideosFlow: FlowShape[(ExistingMovie, UdtValue), (ExistingMovie, UdtValue)] = builder.add(CassandraFlow.create(
      CassandraWriteSettings.defaults,
      insertVideos,
      statementBinder))

    udtValSource ~> mergePreferred ~> zip.in1
    zip.out ~> broadcast
    broadcast.out(0) ~> insertVideosFlow
    broadcast.out(1) ~> extractUdtVal ~> mergePreferred.preferred

    new FlowShape(zip.in0, insertVideosFlow.out)
  }

  private val moviesSourceGraph = Source.fromGraph {
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val genericRecordsShape = builder.add(genericRecordsValidator)
      val movieProcessorShape = builder.add(movieValidator)
      val insertMovieShape = builder.add(insertMovie)

      genericRecordSource ~> genericRecordsShape.in
      genericRecordsShape.out0 ~> movieProcessorShape.in
      movieProcessorShape.out0 ~> insertMovieShape
      movieProcessorShape.out1 ~> rejectedMovieSink
      genericRecordsShape.out1 ~> rejectedRecordsSink
      SourceShape(insertMovieShape.out)
    }
  }

//  private val insertItemByBrandType: Flow[Movie, Movie, NotUsed] = {
//    CassandraFlow.create(CassandraWriteSettings.defaults,
//      s"INSERT INTO user_keyspace.item_by_brand_type(itemId, itemType, brand, description, dimensions, price) VALUES (?, ?, ?, ?, ?, ?)",
//      statementBinder)
//  }

//  private val insertItemByBrandId: Flow[Movie, Movie, NotUsed] = {
//    CassandraFlow.create(CassandraWriteSettings.defaults,
//      s"INSERT INTO user_keyspace.item_by_brand_id(itemId, itemType, brand, description, dimensions, price) VALUES (?, ?, ?, ?, ?, ?)",
//      statementBinder)
//  }
}
