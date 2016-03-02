# Geotrellis Markdown Index

Getting started with any library is a frustrating — often intimidating —
experience. GeoTrellis is not an exception: perhaps you're not a Scala
developer (or a developer at all) or maybe you know your way around code
but are new to the specific domain of GIS software. Whatever the case,
the collected notes, use-cases, and examples in this folder are intended
to help you grapple with the various components which make up
GeoTrellis.

![Raster vs Vector](./img/596px-Raster_vector_tikz.png)

- [geotrellis.etl](etl/etl-intro.md)
- [geotrellis.gdal](gdal/gdal-intro.md) // incomplete
- [geotrellis.proj4](proj4/proj4-intro.md) // incomplete
- [geotrellis.raster](raster/raster-intro.md)
  - [cell types](raster/celltype.md)
  - [cloud removal](raster/cloud-removal.md)
  - [interpolation](raster/interpolation.md) // planned
  - [map algebra](raster/map-algebra.md)
  - [raster input/output](raster/raster-io.md) // planned
  - [rendering](raster/rendering.md)
  - [resampling](raster/resampling.md)
- [geotrellis.slick](spark/slick-intro.md) // planned
- [geotrellis.spark](spark/spark-intro.md) // planned
  - [indexing with the hilbert curve](spark/hilbert-index.md)
  - [map algebra (on spark)](spark/map-algebra.md)
- [geotrellis.spark-etl](spark/spark-intro.md)
- [geotrellis.vector](vector/vector-intro.md)
  - [geojson support](vector/geojson-support.md)
  - [kriging interpolation](vector/kriging-interpolation.md)



## Rasters

Information pertaining to operations on and with raster data

## Vectors

Information pertaining to operations on and with vector data

## Distributing Work

Information pertaining to the use of spark to distribute work across
commodity hardware

## Ingesting Data

Documentation around the provided ETL (extract, transform, and
load) facilities and their extension.

## Misc.

Documentation which doesn't neatly fit the package structure of
GeoTrellis.

