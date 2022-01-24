//import akka.{Done, NotUsed}
//
//import scala.jdk.CollectionConverters._
//import akka.actor.{Actor, ActorLogging, ActorSystem}
//import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
//import akka.kafka.scaladsl.{Consumer, Producer}
//import akka.stream.alpakka.cassandra.{CassandraSessionSettings, CassandraWriteSettings}
//import akka.stream.alpakka.cassandra.scaladsl.{CassandraFlow, CassandraSession, CassandraSessionRegistry}
//import akka.stream.{ActorMaterializer, ClosedShape, FanOutShape2, FlowShape, IOResult, SourceShape}
//import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Keep, MergePreferred, RunnableGraph, Sink, Source, Zip}
//import akka.util.ByteString
//import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
//import com.datastax.oss.driver.api.core.data.UdtValue
//import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet
//import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroDeserializer, KafkaAvroSerializer}
//import model.{EmptyMovie, Encoding, ExistingMovie, Movie}
//import org.apache.avro.Schema.Parser
//import org.apache.avro.data.TimeConversions
//import org.apache.avro.generic.{GenericData, GenericRecord}
//import org.apache.avro.io.{BinaryEncoder, DatumReader, Decoder, DecoderFactory, EncoderFactory}
//import org.apache.avro.specific.{SpecificData, SpecificDatumReader, SpecificDatumWriter, SpecificRecord}
//import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.kafka.common.serialization._
//import org.apache.kafka.common.utils.Bytes
//
//import java.io.{ByteArrayOutputStream, File}
//import java.util.{Date, Properties, UUID}
//import java.util.UUID.randomUUID
//import org.apache.avro.io.Decoder
//import org.apache.avro.io.DecoderFactory
//import org.apache.avro.util.Utf8
//
//import java.nio.charset.StandardCharsets
//import java.nio.file.Paths
//import java.text.DateFormat
//import java.time.LocalDate
//import java.time.format.DateTimeFormatter
//import scala.collection.mutable
//import scala.concurrent.Future
//import scala.util.{Failure, Success}
//
//case class TestMovie(id: UUID)
//
//object TestConsumer extends App {
//  import utilities.Validations._
//
//  implicit val system = ActorSystem()
//  implicit val executionContext = system.dispatcher
//  private val sessionSettings: CassandraSessionSettings = CassandraSessionSettings()
//  implicit val cassandraSession: CassandraSession = CassandraSessionRegistry.get(system).sessionFor(sessionSettings)
//  val meta = cassandraSession.serverMetaData
//  meta.onComplete {
//    case Success(value) => println(s"Success with meta: $value")
//    case Failure(exception) => println(s"Cannot connect to cassandra: $exception")
//  }
//  private def genericConsumerSettings(broker: String, schemaRegistry: String, groupId: String): ConsumerSettings[String, GenericRecord] = {
//    val kafkaAvroSerDeConfig = Map[String, Any] {
//      AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> schemaRegistry
//    }
//    val kafkaAvroDeserializer = new KafkaAvroDeserializer()
//    kafkaAvroDeserializer.configure(kafkaAvroSerDeConfig.asJava, false)
//    val deserializer = kafkaAvroDeserializer.asInstanceOf[Deserializer[GenericRecord]]
//
//    ConsumerSettings(system, new StringDeserializer, deserializer)
//      .withBootstrapServers(broker)
//      .withGroupId(groupId)
//      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//  }
//
//  //DB Statements
//  private val insertVideos = "INSERT INTO moovie.videos(video_id, avg_rating, description, encoding, genres, mpaa_rating, preview_thumbnails, release_date, tags, title, type, url, user_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
//
//  //rejection files
//  private val rejectionFile = Paths.get("src/main/resources/rejected_records.txt")
//  private val rejectionMovie = Paths.get("src/main/resources/rejected_movies.txt")
//
//  private val consumerSettings = genericConsumerSettings("localhost:9092", "http://localhost:8081", randomUUID().toString())
//
//  //Sources, Flows and Sinks
//  private val kafkaSource: Source[ConsumerRecord[String, GenericRecord], Consumer.Control] =
//    Consumer.plainSource(consumerSettings, Subscriptions.topics("movies"))
//  private val genericRecordSource = kafkaSource.map(_.value())
//  private val rejectedMovieSink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(rejectionMovie)
//  private val rejectedRecordsSink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(rejectionFile)
//
//  private val statementBinder: ((ExistingMovie, UdtValue), PreparedStatement) => BoundStatement =
//    (movieTuple, preparedStatement) => {
//      val movie = movieTuple._1
//      val udtV = movieTuple._2
//      udtV.setString("encoding", movie.encoding.encoding)
//      udtV.setInt("height", movie.encoding.height)
//      udtV.setInt("width", movie.encoding.width)
//      preparedStatement.bind(
//        movie.video_id,
//        movie.avg_rating,
//        movie.description,
//        udtV,
//        movie.genres,
//        movie.mpaa_rating,
//        movie.preview_thumbnails,
//        movie.release_date,
//        movie.tags,
//        movie.title,
//        movie.movie_type,
//        movie.url,
//        movie.user_id
//      )
//    }
//
//  private val genericRecordsValidator = GraphDSL.create() { implicit builder =>
//    import GraphDSL.Implicits._
//
//    val broadcastGeneric = builder.add(Broadcast[GenericRecord](2))
//    val validRecords = builder.add(Flow[GenericRecord].filter(record => isValidRecord(record)))
//    val rejectedRecords = builder.add(Flow[GenericRecord].filter(record => !isValidRecord(record)))
//    val toByteString = builder.add(Flow[GenericRecord].map[ByteString](genRecord => ByteString(genRecord.toString)))
//
//    broadcastGeneric.out(0) ~> validRecords
//    broadcastGeneric.out(1) ~> rejectedRecords ~> toByteString
//
//    new FanOutShape2(broadcastGeneric.in, validRecords.out, toByteString.out)
//  }
//
//  private val movieValidator = GraphDSL.create() { implicit builder =>
//    import GraphDSL.Implicits._
//
//    val toMovieShape = builder.add(Flow[GenericRecord].map(toMovie))
//    val broadcastMovie = builder.add(Broadcast[(GenericRecord, Movie)](2))
//    val validMovie = builder.add(Flow[(GenericRecord, Movie)].filterNot(_._2.isEmpty).map(_._2))
//    val existingMovie = builder.add(Flow[Movie].map(movie => movie.asInstanceOf[ExistingMovie]))
//    val rejectedMovie = builder.add(Flow[(GenericRecord, Movie)].filter(_._2.isEmpty).map(_._1))
//    val toByteString = builder.add(Flow[GenericRecord].map[ByteString](genRecord => ByteString(genRecord.toString)))
//
//    toMovieShape.out ~> broadcastMovie
//                        broadcastMovie.out(0) ~> validMovie ~> existingMovie
//                        broadcastMovie.out(1) ~> rejectedMovie ~> toByteString
//
//    new FanOutShape2(toMovieShape.in, existingMovie.out, toByteString.out)
//  }
//
//  private val insertMovie = GraphDSL.create() { implicit builder =>
//    import GraphDSL.Implicits._
//
//    val udtValFuture = cassandraSession.underlying().map { session =>
//      session.getMetadata
//        .getKeyspace("moovie")
//        .get()
//        .getUserDefinedType("video_encoding")
//        .get()
//        .newValue()
//    }
//    val udtValSource = Source.future(udtValFuture)
//    val mergePreferred = builder.add(MergePreferred[UdtValue](1))
//    val zip = builder.add(Zip[ExistingMovie, UdtValue]())
//    val broadcast = builder.add(Broadcast[(ExistingMovie, UdtValue)](2))
//    val extractUdtVal = builder.add(Flow[(ExistingMovie, UdtValue)].map{ case (_, udtvalue) => udtvalue})
//
//    val insertVideosFlow: FlowShape[(ExistingMovie, UdtValue), (ExistingMovie, UdtValue)] = builder.add(CassandraFlow.create(
//      CassandraWriteSettings.defaults,
//        insertVideos,
//        statementBinder))
//
//    udtValSource ~> mergePreferred ~> zip.in1
//    zip.out ~> broadcast
//               broadcast.out(0) ~> insertVideosFlow
//               broadcast.out(1) ~> extractUdtVal ~> mergePreferred.preferred
//
//    new FlowShape(zip.in0, insertVideosFlow.out)
//  }
//
//  private val moviesSourceGraph = Source.fromGraph {
//    GraphDSL.create() { implicit builder =>
//      import GraphDSL.Implicits._
//
//      val genericRecordsShape = builder.add(genericRecordsValidator)
//      val movieProcessorShape = builder.add(movieValidator)
//      val insertMovieShape = builder.add(insertMovie)
//
//      genericRecordSource ~> genericRecordsShape.in
//      genericRecordsShape.out0 ~> movieProcessorShape.in
//                                  movieProcessorShape.out0 ~> insertMovieShape
//                                  movieProcessorShape.out1 ~> rejectedMovieSink
//      genericRecordsShape.out1 ~> rejectedRecordsSink
//      SourceShape(insertMovieShape.out)
//    }
//  }
//  moviesSourceGraph.runForeach(println(_))
//
//}
