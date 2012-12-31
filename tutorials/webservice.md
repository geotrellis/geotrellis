---
layout: tutorials
title: Web Service Tutorial

tutorial: tutorials
num: 2
---

GeoTrellis is commonly used to build web services.

Here we will show you how to build simple web services with Jetty that uses
GeoTrellis.

#### Hello World

This is the most basic web service--in fact, it doesn't directly use GeoTrellis
at all. However, it contains all the imports and objects that the other
examples will need, as well as providing an example of writing a Jetty web
service in Scala.

This service can be accessed at `http://localhost:8000/greeting` and should
print "hello world". You may want to substitude your own values for `mypackage`
and `MyApp`.

    package mypackage

    import javax.servlet.http.HttpServletRequest
    import javax.ws.rs.{GET, Path, DefaultValue, 
                        PathParam, QueryParam}
    import javax.ws.rs.core.{Response, Context}

    import geotrellis._
    import geotrellis.op._
    import geotrellis.process._

    object MyApp {
    val server = Server("myapp", 
                        "src/main/resources/myapp-catalog.json")
    def response(mime:String)(data:Any) = 
        Response.ok(data).`type`(mime).build()
    }

    @Path("/greeting")
    class HelloWorld {
    @GET
    def get(@Context req:HttpServletRequest) = {
        // give a friendly greeting
        MyApp.response("text/plain")("hello world")
    }
    }

#### Adder

With that out of the way, here's a REST service that actually does something:
it treats the final part of the URL as a number, adds one to it, and returns
that number as plaintext. For instance, `http://localhost:8080/adder/127` would
return "128".

The important thing to see here is that `ParseInt` doesn't actually do any
work. It creates an operation which when run will parse an input `String` and
return an `Int`. But running operations isn't the only thing you can do. In our
case, `opX + 1` actually builds us a new operation, which evaluates `opX` and
then adds one to it. If there is an exception (i.e. the parameter is not a
valid number) it will only occur when `MyApp.server.run` is called.

This means that instead of sprinkling error-handling code through your handler,
you only need to guard your calls to `server.run`. Combining operations allows
the web service author to focus on correctly encoding application logic without
having to handle errors at every possible point.

Calling `server.run` returns an integer value which can be passed to the user.

import geotrellis.op.util.string.ParseInt
import geotrellis.Implicits._

    @Path("/adder/")
    class AddOne {
        @GET
        @Path("/{x}")
        def get(@PathParam("x") s:String,
                @Context req:HttpServletRequest) = {
            // parse the given integer
            val opX:Op[Int] = ParseInt(s)

            // add one
            val opY:Op[Int] = opX + 1

            // run the operation
            val data:String = try {
                val y:Int = MyApp.server.run(opY)
                y.toString
            } catch {
                case e => e.toString
            }

            MyApp.response("text/plain")(data)
        }
    }

#### Bounding Box Union

This example is similar to the previous one, but a bit more complicated: it
uses `geotrellis.Extent`, an object which represents a geographical bounding box.
The service uses two extents, each encoded in a string as
`xmin,ymin,xmax,ymax`. For example, using the Web Mercator coordinate system a
bounding box around Philadelphia might be encoded as:

`-8475497.88486,4825540.69147,-8317922.88486,4954765.69147`

This service takes two such extents and combines them, returning the smallest
extent that contains both of them. For instance, the request:

`http://localhost:8000/bbox/0,0,10,10/union/5,-10,15,0`

will result in `0,-10,15,10`.


    import geotrellis.op.raster.extent

    @Path("/bbox/")
    class BoundingBox {
        @GET
        @Path("/{extent1}/union/{extent2}")
        def get(@PathParam("extent1") s1:String,
                @PathParam("extent2") s2:String,
                @Context req:HttpServletRequest) = {
          // parse the given extents
          val e1:Op[Extent] = extent.ParseExtent(s1)
          val e2:Op[Extent] = extent.ParseExtent(s2)

          // combine the extents
          val op:Op[Extent] = extent.CombineExtents(e1, e2)

          // run the operation
          val data = try {
            val extent:Extent = MyApp.server.run(op)
            extent.toString
          } catch {
            case e => e.toString
          }

          MyApp.response("text/plain")(data)
        }
    }

#### Draw Raster

Finally, we will actually do some raster processing! This example shows how to
load raster data, create a palette of colors, assign those colors to value
rangers (classes) in the raster, and create a PNG to send to the user. Whenever
you are rendering an image for a user, you will probably need to go through
these same steps (although you may choose to hardcode the colors). 

Given a `name`, the `LoadRaster` operation will return a `geotrellis.IntRaster`
instance containing the appropriate raster data. This class represents a file
of raster data in the `arg32` format. If you have raster data in another format
(e.g. `.tif` or `.asc`) it will need to be converted ahead of time.

The `palette` and `shades` arguments are used to construct the color palette to
use. For example:

`http://localhost:8000/draw/foo/palette/ff0000,0000ff/shades/3`

specifies that the palette should be a gradient from red (`ff0000`) to blue
(`0000ff`) containing 5 colors. The colors chosen in this case would be:

 * red (`ff0000`)
 * reddish-purple (`bf003f`)
 * purple (`7f007f`)
 * bluish-purple (`3f00bf`)
 * blue (`0000ff`)

The rest of the code builds a `geotrellis.stat.Histogram` object (using
`BuildMapHistogram`) to determine what value ranges should map to which colors
(using `GetColorsBreaks` to create a `geotrellis.data.ColorBreaks`). A full
explanation of the methodology is beyond the scope of this tutorial, but the
basic idea is to try to find ranges of equal size in the raster, so that the 5
colors are evenly used. Thus, assuming our values ranged from 0-100, we might
create the following ranges (also know as "breaks"): 

 * 0-12: red
 * 13-30: reddish-purple
 * 31-36: purple
 * 37-60: bluish-purple
 * 61-100: blue

Finally, we render the PNG using the raster and color breaks we found.

Again, remember that none of this work is happening until after the "run the
operation" comment. This means that any errors (e.g. invalid colors, invalid
raster, other problems) won't happen until that point. It also means that we
could as many raster transformations as we want without needing to modify any
of the rendering code, or the error-handling.   

One thing to note is that this code is not doing any kind of resampling or
resizing. In your own code, you will usually want to load raster data for a
particular extent (e.g. a tile) at a particular resolution (e.g. 256x256). This
services loads and renders the entire raster at its underlying resolution,
which can be slower.

    import geotrellis.op.util.string.{SplitOnComma,ParseInt}

    @Path("/draw/")
    class DrawRaster {
      @GET
      @Path("/{name}/palette/{palette}/shades/{shades}")
      def get(@PathParam("name") name:String,
              @PathParam("palette") palette:String,
              @PathParam("shades") shades:String,
              @Context req:HttpServletRequest) = {

        // load the raster
        val rasterOp = raster.data.LoadRaster(name)

        // find the colors to use
        val paletteOp = 
          logic.ForEach(SplitOnComma(palette))(ParseInt(_,16))
        val numOp = ParseInt(shades)
        val colorsOp = 
          raster.stat.GetColorsFromPalette(paletteOp, numOp)

        // find the appropriate quantile class breaks to use
        val histogramOp = raster.stat.Histogram(rasterOp)
        val breaksOp = 
          raster.stat.GetColorBreaks(histogramOp, colorsOp)

        // render the png
        val pngOp = 
          raster.data.RenderPNG(rasterOp, breaksOp, 0, true)

        // run the operation
        try {
          val img:Array[Byte] = MyApp.server.run(pngOp)
          MyApp.response("image/png")(img)
        } catch {
          case e => MyApp.response("text/plain")(e.toString)
        }
      }
    }

#### Conclusion

These services are all toys, but together they illustrate some of the concepts
used by GeoTrellis. For a more complete example please see
`geotrellis.rest.Demo` in the `demo` project which implements a more complete
weighted overlay service.
