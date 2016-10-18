package geotrellis.spark.mapalgebra.local

import geotrellis.raster._
import geotrellis.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

object Implicits extends Implicits

trait Implicits {
  implicit class withLocalTileRDDMethods[K](val self: RDD[(K, Tile)])
    (implicit val keyClassTag: ClassTag[K]) extends LocalTileRDDMethods[K]

  implicit class withLocalTileRDDSeqMethods[K](val self: Traversable[RDD[(K, Tile)]])
    (implicit val keyClassTag: ClassTag[K]) extends LocalTileRDDSeqMethods[K]

  implicit class withLocalTileDatasetMethods[K](val self: Dataset[(K, Tile)])
    (implicit val keyClassTag: ClassTag[K]) extends LocalTileDatasetMethods[K]

  implicit class withLocalTileDatasetSeqMethods[K](val self: Traversable[Dataset[(K, Tile)]])
    (implicit val keyClassTag: ClassTag[K]) extends LocalTileDatasetSeqMethods[K]
}