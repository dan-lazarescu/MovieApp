package service

import org.slf4j.LoggerFactory
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpHeader, HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet
import model.{Encoding, ExistingMovie, Item}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import spray.json._
import model.MoviesRepo.{FindAllItems, FindItem, FindItemByBrand, FindItemByBrandId}
import utilities.Validations._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID

trait ItemStoreJsonProtocol extends DefaultJsonProtocol {
  implicit val itemFormat = jsonFormat6(Item)
}

trait MovieJsonProtocol extends DefaultJsonProtocol {
  implicit val encodingFormat = jsonFormat3(Encoding)

  implicit object UUIDFormat extends JsonFormat[UUID] {
    def write(uuid: UUID) = JsString(uuid.toString)
    def read(value: JsValue): UUID = value match {
        case JsString(uuid) => UUID.fromString(uuid)
        case _ => throw DeserializationException("Expected hexadecimal UUID string")
      }
  }

  implicit object LocalDateFormat extends JsonFormat[LocalDate] {
    def write(someDate: LocalDate): JsArray = JsArray(JsString(someDate.toString))
    def read(value: JsValue): LocalDate = value match {
      case JsArray(Vector(JsString(someDate))) =>
        LocalDate.parse(someDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
      case _ => deserializationError("LocalDate expected")
    }
  }

  implicit object ImmutableSetFormat extends JsonFormat[ImmutableSet[String]] {
    def write(someSet: ImmutableSet[String]): JsArray = JsArray(JsString(someSet.toString))

    def read(value: JsValue): ImmutableSet[String] = value match {
      case JsArray(Vector(JsString(someString))) => toImmutableSetFromString(someString)
      case _ => deserializationError("ImmutableSet expected")
    }
  }

  implicit object JavaMapFormat extends JsonFormat[java.util.Map[String, String]] {
    def write(someMap: java.util.Map[String, String]): JsArray =
      JsArray(JsString(someMap.))

    def read(value: JsValue): ImmutableSet[String] = value match {
      case JsArray(Vector(JsString(someString))) => toImmutableSetFromString(someString)
      case _ => deserializationError("ImmutableSet expected")
    }
  }

  implicit val movieFormat = jsonFormat13(ExistingMovie)
}

class WebService(dbActor: ActorRef) extends ItemStoreJsonProtocol {
  implicit val actSysytem = ActorSystem()
  implicit val ec = ExecutionContext.global
  implicit val timeout = Timeout(Duration(1000, "millis"))

  val logger = LoggerFactory.getLogger(classOf[WebService])
  private def toHttpEntity(payload: String): HttpEntity.Strict = HttpEntity(ContentTypes.`application/json`, payload)

  private val itemsRoute: Route =
    pathPrefix("api" / "item") {
      get {
        (path(IntNumber) | parameter("id".as[Int])) { itemId =>
          complete(
            (dbActor ? FindItem(itemId))
              .mapTo[Option[Item]]
              .map(_.toJson.prettyPrint)
              .map(toHttpEntity)
          )
        } ~
        (parameter("brand".as[String]) & parameter("brand_id".as[Int])) { (brand, id) =>
          val fut = (dbActor ? FindItemByBrandId(brand, id)).mapTo[(List[Item], String)]
          onComplete(fut) {
            case Success(someItems) =>
              val resultItems = someItems._1
              val nextItemId = someItems._2
              if (resultItems.isEmpty) {
                complete(StatusCodes.NoContent)
              } else {
                respondWithHeader(RawHeader("Next-Item", nextItemId)) {
                  complete(
                    toHttpEntity(resultItems.toJson.prettyPrint)
                  )
                }
              }
            case Failure(ex) =>
              logger.error("Exception encountered:", ex)
              complete(StatusCodes.BadRequest)
          }

        } ~
        parameter("brand".as[String]) { brand =>
          complete(
            (dbActor ? FindItemByBrand(brand))
              .mapTo[List[Item]]
              .map(_.toJson.prettyPrint)
              .map(toHttpEntity)
          )
        } ~
        pathEndOrSingleSlash {
          complete(
            (dbActor ? FindAllItems)
              .mapTo[List[Item]]
              .map(_.toJson.prettyPrint)
              .map(toHttpEntity)
          )
        }
      }
    }

  def startServer(): Future[Http.ServerBinding] = Http().newServerAt("localhost", 8080).bind(itemsRoute)

}
