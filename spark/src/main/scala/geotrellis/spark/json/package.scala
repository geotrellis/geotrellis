package geotrellis.spark

import geotrellis.spark.tiling._
import geotrellis.raster._
import geotrellis.raster.json._
import geotrellis.vector.Extent
import geotrellis.vector.json._
import geotrellis.proj4.CRS

import spray.json._

package object json {
  implicit object CRSFormat extends RootJsonFormat[CRS] {
    def write(crs: CRS) =
      JsString(crs.toProj4String)

    def read(value: JsValue): CRS = 
      value match {
        case JsString(proj4String) => CRS.fromString(proj4String)
        case _ => 
          throw new DeserializationException("CRS must be a proj4 string.")
      }
  }

  implicit object LayerIdFormat extends RootJsonFormat[LayerId] {
    def write(id: LayerId) =
      JsObject(
        "name" -> JsString(id.name),
        "zoom" -> JsNumber(id.zoom)
      )

    def read(value: JsValue): LayerId =
      value match {
        case Seq(JsString(name), JsNumber(zoom)) =>
          LayerId(name, zoom.toInt)
        case _ =>
          throw new DeserializationException("LayerId expected")
      }
  }

  implicit object RasterMetaDataFormat extends RootJsonFormat[RasterMetaData] {
    def write(metaData: RasterMetaData) = 
      JsObject(
        "cellType" -> metaData.cellType.toJson,
        "extent" -> metaData.extent.toJson,
        "crs" -> metaData.crs.toJson,
        "tileLayout" -> metaData.tileLayout.toJson
      )

    def read(value: JsValue): RasterMetaData =
      value.asJsObject.getFields("cellType", "extent", "crs", "layoutLevel") match {
        case Seq(cellType, extent, crs, tileLayout, JsString(gridIndex)) =>
          RasterMetaData(
            cellType.convertTo[CellType],
            extent.convertTo[Extent],
            crs.convertTo[CRS],
            tileLayout.convertTo[TileLayout]
          )
        case _ =>
          throw new DeserializationException("RasterMetaData expected")
      }
  }


  implicit object LayerMetaDataFormat extends RootJsonFormat[LayerMetaData] {
    def write(metaData: LayerMetaData) = 
      JsObject(
        "id" -> metaData.id.toJson,
        "rasterMetaData" -> metaData.rasterMetaData.toJson
      )

    def read(value: JsValue): LayerMetaData =
      value.asJsObject.getFields("id", "rasterMetaData") match {
        case Seq(layerId, rasterMetaData) =>
          LayerMetaData(
            layerId.convertTo[LayerId],
            rasterMetaData.convertTo[RasterMetaData]
          )
        case _ =>
          throw new DeserializationException("LayerMetaData expected")
      }
  }
}
