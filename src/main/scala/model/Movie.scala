package model

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet

import java.time.LocalDate
import java.util.{Date, UUID}

trait Movie {
  def isEmpty: Boolean
}

case class Item(itemId: Int, brand: String, itemType: String, description: String, dimensions: String, price: Float)
case class Encoding(encoding: String, height: Int, width: Int)

class EmptyMovie extends Movie {
  override def isEmpty: Boolean = true
}
case class ExistingMovie(video_id: UUID,
                 avg_rating: Float,
                 description: String,
                 encoding: Encoding,
                 genres: ImmutableSet[String],
                 mpaa_rating: String,
                 preview_thumbnails: java.util.Map[String, String],
                 release_date: LocalDate,
                 tags: ImmutableSet[String],
                 title: String,
                 movie_type: String,
                 url: String,
                 user_id: UUID
                ) extends Movie {
  override def isEmpty: Boolean = false
}