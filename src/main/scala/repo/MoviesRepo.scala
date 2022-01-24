package repo

import akka.actor.Actor
import akka.pattern.pipe
import akka.stream.Materializer
import akka.stream.alpakka.cassandra.scaladsl.{CassandraSession, CassandraSource}
import akka.stream.scaladsl.Sink
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, Row, SimpleStatement}
import com.datastax.oss.driver.api.core.data.UdtValue
import model.{ExistingMovie, Item}
import org.slf4j.LoggerFactory
import model.MoviesRepo.{FindAllItems, FindItem, FindItemByBrand, FindItemByBrandId}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

object MoviesRepo {
  val itemByBrandType = "user_keyspace.item_by_brand_type"
  val itemByBrandId = "user_keyspace.item_by_brand_id"
  val itemById = "user_keyspace.item_by_id"
  val allVideos = "moovie.videos"

  //DB Statements
  val insertVideos = "INSERT INTO moovie.videos(video_id, avg_rating, description, encoding, genres, mpaa_rating, preview_thumbnails, release_date, tags, title, type, url, user_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

  val statementBinder: ((ExistingMovie, UdtValue), PreparedStatement) => BoundStatement =
    (movieTuple, preparedStatement) => {
      val movie = movieTuple._1
      val udtV = movieTuple._2
      udtV.setString("encoding", movie.encoding.encoding)
      udtV.setInt("height", movie.encoding.height)
      udtV.setInt("width", movie.encoding.width)
      preparedStatement.bind(
        movie.video_id,
        movie.avg_rating,
        movie.description,
        udtV,
        movie.genres,
        movie.mpaa_rating,
        movie.preview_thumbnails,
        movie.release_date,
        movie.tags,
        movie.title,
        movie.movie_type,
        movie.url,
        movie.user_id
      )
    }
}

class MoviesRepo(implicit val cassandraSession: CassandraSession) extends Actor {
  import MoviesRepo._
  implicit val mat: Materializer = Materializer(context)
  implicit val ec = ExecutionContext.global
  val logger = LoggerFactory.getLogger(classOf[MoviesRepo])

  override def receive: Receive = {
    case FindAllItems =>
//      val stmt = SimpleStatement.newInstance(s"SELECT * FROM $itemById").setPageSize(50)
      val stmt = SimpleStatement.newInstance(s"SELECT * FROM $allVideos").setPageSize(50)
      val rows: Future[immutable.Seq[Row]] = CassandraSource(stmt).runWith(Sink.seq)
      val all = rows.map { rowSeq =>
        val allRows = rowSeq.map { row => buildItem(row) }
        allRows.toList
      }
      logger.info(s"Send response...")
      all pipeTo sender()

    case FindItemByBrand(brand) =>
      val stmtBrand = SimpleStatement.newInstance(s"SELECT * FROM $itemByBrandId WHERE BRAND='$brand' LIMIT 3")
      val rows: Future[immutable.Seq[Row]] = CassandraSource(stmtBrand).runWith(Sink.seq)
      val all = rows.map { rowSeq =>
        val allRows = rowSeq.map { row => buildItem(row) }
        allRows.toList
      }
      logger.info(s"Send response for brand $brand")
      all pipeTo sender()

    case FindItemByBrandId(brand, id) =>
      val stmtBrandId = SimpleStatement.newInstance(
        s"SELECT * FROM $itemByBrandId WHERE BRAND='$brand' and ITEMID>$id LIMIT 3"
      )
      val rows: Future[immutable.Seq[Row]] = CassandraSource(stmtBrandId).runWith(Sink.seq)
      val all = rows.map { rowSeq =>
        val allRows = rowSeq.map { row => buildItem(row) }
        val lastItemId = {
          if (allRows.isEmpty) {
            -1
          } else {
            allRows.last.itemId
          }
        }
        (allRows.toList, lastItemId.toString)
      }
      logger.info(s"Send response for brand $brand")
      all pipeTo sender()

    case FindItem(id) =>
      val stmId = SimpleStatement.newInstance(s"SELECT * FROM $itemById where itemId = $id ")
      val rowFuture: Future[Seq[Row]] = CassandraSource(stmId).runWith(Sink.seq)

      val rowOption: Future[Option[Item]] = rowFuture.map { rowSeq =>
        if (rowSeq.isEmpty) {
          logger.info(s"No item found for id $id")
          None
        } else {
          val result: Seq[Item] = rowSeq.map { row => buildItem(row) }
          Some(result.head)
        }
      }
      logger.info(s"Send response for id $id")
      rowOption pipeTo sender()
  }

  private def buildItem(row: Row): Item = {
    Item(
      row.getInt("itemId"),
      row.getString("brand"),
      row.getString("itemType"),
      row.getString("description"),
      row.getString("dimensions"),
      row.getFloat("price")
    )
  }

}