package geotrellis.store

import geotrellis.layer.{SpatialComponent, SpatialKey, TileLayerMetadata, ZoomedLayoutScheme}
import geotrellis.raster.resample.{ResampleMethod, TileResampleMethods}
import geotrellis.raster.{CellGrid, RasterExtent}
import geotrellis.store._
import geotrellis.store.avro.AvroRecordCodec
import geotrellis.util._

import io.circe._

import scala.reflect.ClassTag
import java.net.URI

trait OverzoomingValueReader extends ValueReader[LayerId] {
  def overzoomingReader[
    K: AvroRecordCodec: Decoder: SpatialComponent: ClassTag,
    V <: CellGrid[Int]: AvroRecordCodec: ? => TileResampleMethods[V]
  ](layerId: LayerId, resampleMethod: ResampleMethod): Reader[K, V] = new Reader[K, V] {
    val LayerId(layerName, requestedZoom) = layerId
    val maxAvailableZoom = attributeStore.availableZoomLevels(layerName).max
    val metadata = attributeStore.readMetadata[TileLayerMetadata[K]](LayerId(layerName, maxAvailableZoom))

    val layoutScheme = ZoomedLayoutScheme(metadata.crs, metadata.tileRows)
    val requestedMaptrans = layoutScheme.levelForZoom(requestedZoom).layout.mapTransform
    val maxMaptrans = metadata.mapTransform

    lazy val baseReader = reader[K, V](layerId)
    lazy val maxReader = reader[K, V](LayerId(layerName, maxAvailableZoom))

    def read(key: K): V =
      if (requestedZoom <= maxAvailableZoom) {
        baseReader.read(key)
      } else {
        val maxKey = {
          val srcSK = key.getComponent[SpatialKey]
          val denom = math.pow(2, requestedZoom - maxAvailableZoom).toInt
          key.setComponent[SpatialKey](SpatialKey(srcSK._1 / denom, srcSK._2 / denom))
        }

        val toResample = maxReader.read(maxKey)

        toResample.resample(
          maxMaptrans.keyToExtent(maxKey.getComponent[SpatialKey]),
          RasterExtent(requestedMaptrans.keyToExtent(key.getComponent[SpatialKey]), toResample.cols, toResample.rows),
          resampleMethod
        )
      }
  }
}
