package geotrellis.raster.op.data

import geotrellis._

/**
 * Converts a raster to an integer array.
 */
case class AsArray(r:Op[Raster]) extends Op1(r)({ 
  r => 
    val data = r.data.asArray
    Result(data.toArray)
})

/**
 * Converts a raster to a double array.
 */
case class AsArrayDouble(r:Op[Raster]) extends Op1(r)({
  r =>  
    val data = r.data.asArray
    Result(data.toArrayDouble)
})
