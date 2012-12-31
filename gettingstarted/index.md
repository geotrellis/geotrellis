---
layout: gettingstarted
title: Introduction

tutorial: gettingstarted
num: 1
outof: 5
---
*GeoTrellis* is a high performance geoprocessing engine and programming
toolkit. The goal of the project is to transform user interaction with
geospatial data by bringing the power of geospatial analysis to real time,
interactive web applications.

GeoTrellis was designed to solve three core problems, with a focus on raster
processing:

- Creating scalable, high performance geoprocessing web services
- Creating distributed geoprocessing services that can act on large data sets
- Parallelizing geoprocessing operations to take full advantage of multi-core
  architecture 

GeoTrellis is a project of Azavea (www.azavea.com), and was written by Josh
Marcus (jmarcus@azavea.com) and Erik Osheim (eosheim@azavea.com). Please
contact us if you have any questions!

#### Features

- GeoTrellis is designed to help a developer create simple, standard REST
  services that return the results of geoprocessing models.
- Like an RDBS that can optimize queries, GeoTrellis will automatically
  parallelize and optimize your geoprocessing models where possible.  
- In the spirit of the object-functional style of Scala, it is easy to both
  create new operations and compose new operations with existing operations.

#### Some sample GeoTrellis code


    // import the necessary stuff
    import geotrellis._
    import geotrellis.op.raster._

    // set up the rasters and weights we'll use
    val raster1 = io.LoadRaster("foo")
    val raster2 = io.LoadRaster("bar")
    val weight1 = 5
    val weight2 = 2

    val total = weight1 + weight2

    // create a new operation that multiplies each cell of
    // each raster by a weight, and then add those two new
    // rasters together.
    val op = local.Add(local.MultiplyConstant(raster1, weight1),
                     local.MultiplyConstant(raster2, weight2))

    // create a new operation that takes the result of the
    // previous operation and divides each cell by the total
    // weight, creating a weighted overlay of our two rasters.
    val wo1 = local.DivideConstant(op, total)

    // we can use a simpler syntax if we want. note that this
    // is still just creating an operation.
    import geotrellis.Implicits._
    val wo2 = (raster1 * weight1 + raster2 * weight2) / total

    // to this point, we've only been composing new operations.
    // now we will run them.
    import geotrellis.process.Server
    val server = Server("example")
    val result1 = server.run(wo1)
    val result2 = server.run(wo2)

