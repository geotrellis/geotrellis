package geotrellis.feature.rasterize

import geotrellis.feature._
import geotrellis._



object Rasterizer {

  /**
   * Create a raster from a geometry feature.
   * @param feature       Feature to rasterize
   * @param rasterExtent  Definition of raster to create
   * @param f             Function that returns single value to burn
   */ 
  def rasterizeWithValue[D:Manifest](feature:Geometry[D], rasterExtent:RasterExtent)(f:(D) => Int) = {
    val cols = rasterExtent.cols
    val array = Array.fill[Int](rasterExtent.cols * rasterExtent.rows)(NODATA)
    val burnValue = f(feature.data)
    val f2 = (col:Int,row:Int,polygon:Geometry[D]) => array(row * cols + col) = burnValue
    foreachCellByFeature(feature, rasterExtent)(f2) 
    Raster(array,rasterExtent)
  } 
  /**
   * Create a raster from a geometry feature.
   * @param feature       Feature to rasterize
   * @param rasterExtent  Definition of raster to create
   * @param f             Function that takes col, row, feature and returns value to burn
   */ 
  def rasterize[D:Manifest](feature:Geometry[D], rasterExtent:RasterExtent)(f:(Int,Int,Geometry[D]) => Int) = {
    val cols = rasterExtent.cols
    val array = Array.fill[Int](rasterExtent.cols * rasterExtent.rows)(NODATA)
    val f2 = (col:Int, row:Int, polygon:Geometry[D]) => array(row * cols + col) = f(col,row,polygon)
    foreachCellByFeature(feature, rasterExtent)(f2)
    Raster(array,rasterExtent)
  }
   
  /**
   * Perform a zonal summary by invoking a function on each cell under provided features.
   *
   * This function is a closure that returns Unit; all results are a side effect of this function.
   * 
   * Note: the function f should modify a mutable variable as a side effect.  
   * While not ideal, this avoids the unavoidable boxing that occurs when a 
   * Function3 returns a primitive value.
   * 
   * @param feature  Feature for calculation
   * @param re       RasterExtent to use for iterating through cells
   * @param f        A function that takes (col:Int, row:Int, rasterValue:Int, feature:Feature)
   */
  def foreachCellByFeature[D:Manifest](feature:Geometry[D], re:RasterExtent)(f:(Int,Int,Geometry[D]) => Unit):Unit = {
    feature match {
      case p:PointSet[_] => foreachCellByPointSet[D](p.asInstanceOf[PointSet[D]],re)(f)
      case p:Point[_] => foreachCellByPoint[D](p.asInstanceOf[Point[D]],re)(f)
      case p:MultiPoint[_] => foreachCellByMultiPoint[D](p.asInstanceOf[MultiPoint[D]],re)(f)
      case p:MultiLineString[_] => foreachCellByMultiLineString[D](p.asInstanceOf[MultiLineString[D]],re)(f)
      case p:LineString[_] => foreachCellByLineString[D](p.asInstanceOf[LineString[D]],re)(f)
      case p:Polygon[_] => PolygonRasterizer.foreachCellByPolygon[D](p.asInstanceOf[Polygon[D]],re)(f)
      case p:MultiPolygon[_] => foreachCellByMultiPolygon[D](p.asInstanceOf[MultiPolygon[D]],re)(f)
    }
  }
  
  /**
   * Calculate value from raster cell under a point feature using provided function. 
   * 
   * This method follows the general pattern of the feature aggregation methods, even though
  * a point only covers a single raster cell.
   */
  def aggregrateCellsByPoint[D,Z](p:Point[D], r:Raster, start:Z)(f:(Int,D,Z) => Z):Z = {
    val geom = p.geom
    val re = r.rasterExtent
    val cellValue = r.get( re.mapXToGrid(geom.getX()), re.mapYToGrid(geom.getY()))
    f(cellValue,p.data,start)
  }
  
    
  /**
   * Aggregate all points in a PointSet with a fold function that takes data from the feature
   * as well as the raster cell value at each point.
   */
  def aggregrateCellsByPointSet[D,Z](p:PointSet[D],r:Raster, start:Z)(f:(Int,D,Z) => Z):Z = {
    p.foldLeft(start)((z,point) => f(Point.pointToRasterValue(point, r),point.data,z) )
  }
 
  /**
   * Invoke a function on raster cells under a point feature.
   * 
   * The function f is a closure that should alter a mutable variable by side
   * effect (to avoid boxing).  
   */
  def foreachCellByPoint[D](p:Point[D], re:RasterExtent)(f:(Int,Int,Point[D]) => Unit) {
    val geom = p.geom
    val x = re.mapXToGrid(geom.getX())
    val y = re.mapYToGrid(geom.getY())
    f(x,y,p)
  }

  def foreachCellByMultiPoint[D](p:MultiPoint[D], re:RasterExtent)(f:(Int,Int,Point[D]) => Unit) {
    p.flatten.foreach(foreachCellByPoint(_, re)(f))
  }
  /**
   * Invoke a function on each point in a PointSet.
   */
  def foreachCellByPointSet[D](pSet:PointSet[D], re:RasterExtent)(f:(Int,Int,Point[D]) => Unit) {
    pSet.foreach(foreachCellByPoint(_,re)(f))
  }
  
  /**
   * Returns a new PointSet where each point in an input PointSet has its data value updated 
   * by a function that takes as input the raster cell value under the point and the 
   * data value of the feature. 
   */
  def aggregrateCellsByPoint[D,Z:Manifest](p:PointSet[D], r:Raster, start:Z)(f:(Int,D,Z) => Z):PointSet[Z] = {
    val f2 = (p:Point[D]) => {
      val geom = p.geom
      val re = r.rasterExtent
      val cellValue = r.get( re.mapXToGrid(geom.getX()), re.mapYToGrid(geom.getY()))
      Point(p.geom, f(cellValue,p.data,start))
    }
    p.map(f2)
  }
 

  /**
   * Apply function f to every cell contained within MultiLineString.
   * @param g   MultiLineString used to define zone
   * @param re  RasterExtent used to determine cols and rows
   * @param f   Function to apply: f(cols,row,feature)
   */
  def foreachCellByMultiLineString[D](g:MultiLineString[D], re:RasterExtent)(f:(Int,Int,LineString[D]) => Unit) {
    g.flatten.foreach(foreachCellByLineString(_,re)(f))
  }

  /**
   * Apply function f(col,row,feature) to every cell contained within polygon.
   * @param p   Polygon used to define zone
   * @param re  RasterExtent used to determine cols and rows
   * @param f   Function to apply: f(cols,row,feature)
   */
  def foreachCellByPolygon[D](p:Polygon[D], re:RasterExtent)(f:(Int,Int,Polygon[D]) => Unit) {
     PolygonRasterizer.foreachCellByPolygon(p, re)(f)
  }

  /**
   * Apply function f to every cell contained with MultiPolygon.
   *
   * @param p   MultiPolygon used to define zone
   * @param re  RasterExtent used to determine cols and rows
   * @param f   Function to apply: f(cols,row,feature)
   */
  def foreachCellByMultiPolygon[D](p:MultiPolygon[D], re:RasterExtent)(f:(Int,Int,Polygon[D]) => Unit) {
    p.flatten.foreach(PolygonRasterizer.foreachCellByPolygon(_,re)(f))
  }

  /**
   * lines
   */
  //TODO: implement midpoint line algorithm
  // Converts start and end point to grid cells, and draws a line between those two grid cells.
  // Uses DDA or simple incremental algorithm.
  def foreachCellByLineString[D](p:LineString[D], re:RasterExtent)(f:(Int,Int,LineString[D]) => Unit) {
    val geom = p.geom
    val p0 = geom.getCoordinateN(0)
    val p1 = geom.getCoordinateN(1)

    val p0x = re.mapXToGrid(p0.x)
    val p0y = re.mapYToGrid(p0.y)
    val p1x = re.mapXToGrid(p1.x)
    val p1y = re.mapYToGrid(p1.y)
    
    if (p0x < p1x) {
      foreachCellInGridLine(p0x, p0y, p1x, p1y, p, re)(f)
    } else {
      foreachCellInGridLine(p1x, p1y, p0x, p0y, p, re)(f)
    }
  }
  
  //TODO: optimizations, including getting line within raster extent
  //test for horizontal and vertical lines
  private def foreachCellInGridLine[D](x0:Int, y0:Int, x1:Int, y1:Int, p:LineString[D], re:RasterExtent)(f:(Int,Int,LineString[D]) => Unit) { 
    val dy = y1 - y0
    val dx = x1 - x0
    val m:Double = dy / dx
    
    // if a step in x creates a step in y that is greater than one, reverse
    // roles of x and y.
    if (math.abs(m) > 1) {      
      foreachCellInGridLineSteep[D](x0, y0, x1, y1, p, re)(f)
    }
    
    var x:Int = x0
    var y:Double = y0
    
    val ymax = re.extent.ymax
    val ymin = re.extent.ymin
    val xmax = re.extent.xmax
    val xmin = re.extent.xmin

    val cols = re.cols
    val rows = re.rows

    while(x <= x1) {
      //println("x: %d, y: %f".format(x,y))

      val newY:Int = (y + 0.5).asInstanceOf[Int]
      if (x >= 0 && y >= 0 && x < cols && y < rows) {
        f(x, newY, p)
      }
      y += m
      x = x + 1
    }
  }
  
  private def foreachCellInGridLineSteep[D](x0:Int, y0:Int, x1:Int, y1:Int, p:LineString[D], re:RasterExtent)(f:(Int,Int,LineString[D]) => Unit) {
    val dy = y1 - y0
    val dx = x1 - x0
    val m = dy / dx
    
    // if a step in x creates a step in y that is greater than one, reverse
    // roles of x and y.
    if (math.abs(m) > 1) {      
      foreachCellInGridLineSteep[D](x0, y0, x1, y1, p, re)(f)
    }
    
    var x:Int = x0
    var y:Int = y0
    
    val ymax = re.extent.ymax
    val ymin = re.extent.ymin
    val xmax = re.extent.xmax
    val xmin = re.extent.xmin
    
    val cols = re.cols
    val rows = re.rows
    
    val step = 1/m
    while(y <= y1) {
      //println("x: %f, y: %f".format(x,y))
      val newX:Int = (x + 0.5).asInstanceOf[Int]
      if (x >= 0 && y >= 0 && x < cols && y < rows) {        
    	  f(newX, y, p)
      }
      x += step
      y = y + 1
    }
  } 
}
