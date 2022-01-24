package utilities

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet
import model.{EmptyMovie, Encoding, ExistingMovie, Movie}
import org.apache.avro.generic.GenericRecord
import utilities.StringToGenericRecord._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.collection.mutable
import scala.util.{Failure, Success}
import scala.jdk.CollectionConverters._

object Validations {

  private val dateFormat = "yyyy-MM-dd"

  def isValidRecord(record: GenericRecord): Boolean = {
    val schema = record.getSchema
    val someMovie = record.toString.toGenericRecord(schema, true)
    someMovie match {
      case Success(movieData) => true
      case Failure(exception) => false
    }
  }

  def toMovie(genRec: GenericRecord): (GenericRecord, Movie) = {
    try {
      val mov = ExistingMovie(
        UUID.fromString(genRec.get("video_id").toString),
        toFloatFromString(genRec.get("avg_rating").toString),
        genRec.get("description").toString,
        toEncoding(genRec.get("encoding").asInstanceOf[GenericRecord]),
        toImmutableSetFromString(genRec.get("genres").toString),
        genRec.get("mpaa_rating").toString,
        toMapFromString(genRec.get("preview_thumbnails").toString).asJava,
        toDateFromString(genRec.get("release_date").toString, dateFormat),
        toImmutableSetFromString(genRec.get("tags").toString),
        genRec.get("title").toString,
        genRec.get("movie_type").toString,
        genRec.get("url").toString,
        UUID.fromString(genRec.get("user_id").toString))
      (genRec, mov)
    } catch {
      case e: Exception =>
        println(s"Exception $e when converting movie")
        (genRec, new EmptyMovie)
    }
  }

  def toEncoding(encodingRecord: GenericRecord): Encoding = {
    val schema = encodingRecord.getSchema
    val someEncoding = encodingRecord.toString.toGenericRecord(schema, true)
    someEncoding match {
      case Success(encoding) =>
        println(s"Success with encoding!!!")
        Encoding(
          encoding.get("encoding").toString.trim,
          encoding.get("height").toString.trim.toInt,
          encoding.get("width").toString.trim.toInt,
        )
      case Failure(exception) =>
        println(s"Exception $exception")
        Encoding("", 0, 0)
    }
  }

  def toDateFromString(dateString: String, fmt: String): LocalDate = {
    val format = DateTimeFormatter.ofPattern(fmt)
    try {
      LocalDate.parse(dateString, format)
    } catch {
      case e: Exception =>
        println(s"Exception $e for converting $dateString => using system date ${LocalDate.now()}")
        LocalDate.now()
    }
  }

  def toFloatFromString(someString: String): Float = {
    try {
      someString.toFloat
    } catch {
      case e: Exception =>
        println(s"Exception $e for converting $someString => using 0.0f")
        0.0f
    }
  }

  def toImmutableSetFromString(someString: String): ImmutableSet[String] = {
    val stringArray = someString.drop(1).dropRight(1).split(",")
    ImmutableSet.copyOf(stringArray)
  }

  //
  def toMapFromString(someString: String): mutable.Map[String, String] = {

    def helpToMap(fString: String): (String, String) = fString match {
      case "" => ("", "")
      case someString =>
        val arrayString = someString.split(":")
        (arrayString(0), arrayString(1))
    }

    val tupleArray = someString.drop(1).dropRight(1).split(",").map(elem => helpToMap(elem))
    val mapFromString = mutable.Map[String, String]()
    mapFromString ++= tupleArray
    mapFromString
  }

}
