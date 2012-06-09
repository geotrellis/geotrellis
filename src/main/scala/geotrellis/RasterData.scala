package geotrellis

import sys.error

import geotrellis.raster._

object RasterData {
  def largestType(lhs:RasterData, rhs:RasterData) = {
    lhs.getType.union(rhs.getType)
  }
  def largestByType(lhs:RasterData, rhs:RasterData) = {
    if (largestType(lhs, rhs) == lhs.getType) lhs else rhs
  }
  def largestAlloc(lhs:RasterData, rhs:RasterData, size:Int) = {
    largestByType(lhs, rhs).alloc(size)
  }

  def allocByType(t:RasterType, size:Int):StrictRasterData = t match {
    case TypeBit => BitArrayRasterData.ofDim(size)
    case TypeByte => ByteArrayRasterData.ofDim(size)
    case TypeShort => ShortArrayRasterData.ofDim(size)
    case TypeInt => IntArrayRasterData.ofDim(size)
    case TypeFloat => FloatArrayRasterData.ofDim(size)
    case TypeDouble => DoubleArrayRasterData.ofDim(size)
  }
}

/**
 * RasterData provides access and update to the grid data of a raster.
 *
 * Designed to be a near drop-in replacement for Array in many cases.
 */
trait RasterData {
  def getType: RasterType
  def alloc(size:Int): StrictRasterData

  def copy:RasterData
  def length:Int
  def lengthLong:Long 

  //override def toString = "RasterData(<%d values>, type=%s)" format (length,getType)

  def combine2(other:RasterData)(f:(Int,Int) => Int):RasterData
  def foreach(f: Int => Unit):Unit
  def map(f:Int => Int):RasterData
  def mapIfSet(f:Int => Int):RasterData

  def combineDouble2(other:RasterData)(f:(Double,Double) => Double) = {
    combine2(other)((a, b) => f(a, b).toInt)
  }
  def foreachDouble(f: Double => Unit) = foreach(z => f(z))
  def mapDouble(f:Double => Double) = map(z => f(z).toInt)
  def mapIfSetDouble(f:Double => Double) = mapIfSet(z => f(z).toInt)

  /**
   * Return the current RasterData values as a strict (calculated) ArrayRasterData.
   *
   * If your RasterData cannot be represented as an array, bad things will happen.
   * If your RasterData is lazy, any deferred calculations will be executed.
   */
  def force:StrictRasterData
  def defer:LazyRasterData = LazyArrayWrapper(asArray)

  def asArray:ArrayRasterData

  def get(x:Int, y:Int, cols:Int):Int
  def set(x:Int, y:Int, z:Int, cols:Int):Unit 

  def getDouble(x:Int, y:Int, cols:Int):Double
  def setDouble(x:Int, y:Int, z:Double, cols:Int):Unit
}

/**
 * ArrayRasterData provides array-like access to the grid data of a raster.
 *
 * While often backed with a real Array for speed, it cannot be used for rasters
 * with more cells than the maximum size of a java array: 2.14 billion.
 */ 
trait ArrayRasterData extends RasterData {
  //override def toString = "ArrayRasterData(<%d values>, type=%s)" format (length,getType)
  def copy:ArrayRasterData 

  //TODO: RasterData should be lazy by default.
  //def defer:LazyRasterData = LazyArrayWrapper(this)

  def lengthLong = length
  def apply(i: Int):Int
  def update(i:Int, x:Int)

  def toArray:Array[Int]
  def toList = toArray.toList

  // alternate double-based implementations
  def applyDouble(i:Int):Double = apply(i).toDouble
  def updateDouble(i:Int, x:Double):Unit = update(i, x.toInt)
  def toArrayDouble:Array[Double] = toArray.map(_.toDouble)
  def toListDouble = toArrayDouble.toList

  def foreach(f:Int => Unit):Unit = {
    var i = 0
    val len = length
    while(i < len) {
      f(apply(i))
      i += 1
    }
  }
  override def foreachDouble(f:Double => Unit):Unit = {
    var i = 0
    val len = length
    while(i < len) {
      f(applyDouble(i))
      i += 1
    }
  }

  def map(f:Int => Int) = {
    val data = copy
    var i = 0
    val len = length
    while (i < len) {
      data(i) = f(data(i))
      i += 1
    }
    data
  }
  override def mapDouble(f:Double => Double) = {
    val data = copy
    var i = 0
    val len = length
    while (i < len) {
      data.updateDouble(i, f(data.applyDouble(i)))
      i += 1
    }
    data
  }

  def mapIfSet(f:Int => Int) = {
    val data = copy
    var i = 0
    val len = length
    while (i < len) {
      val z = data(i)
      if (z != NODATA) data(i) = f(z)
      i += 1
    }
    data
  }
  override def mapIfSetDouble(f:Double => Double) = {
    val data = copy
    var i = 0
    val len = length
    while (i < len) {
      val z = data.applyDouble(i)
      if (java.lang.Double.isNaN(z)) data.updateDouble(i, f(z))
      i += 1
    }
    data
  }

  // TODO: if profiling finds this to be slow, we could do the trick
  // where we copy this/other and then only iterate over the one we
  // didn't already copy.
  // FIXME: if other isn't an array, we should probably let it combine
  def combine2(rhs:RasterData)(f:(Int,Int) => Int):RasterData = rhs match {
    case other:ArrayRasterData => {
      val output = RasterData.largestAlloc(this, other, length)
      var i = 0
      val len = length
      while (i < len) {
        output(i) = f(this(i), other(i))
        i += 1
      }
      output
    }
    case _ => rhs.combine2(this)((b, a) => f(a, b))
  }
  override def combineDouble2(rhs:RasterData)(f:(Double,Double) => Double) = rhs match {
    case other:ArrayRasterData => {
      val output = RasterData.largestAlloc(this, other, length)
      var i = 0
      val len = length
      while (i < len) {
        output.updateDouble(i, f(this.applyDouble(i), other.applyDouble(i)))
        i += 1
      }
      output
    }
    case _ => rhs.combineDouble2(this)((b, a) => f(a, b))
  }

  override def equals(other:Any):Boolean = other match {
    case r:ArrayRasterData => {
      if (r == null) return false
      val len = length
      if (len != r.length) return false
      var i = 0
      while (i < len) {
        if (apply(i) != r(i)) return false
        i += 1
      }
      true
    }
    case _ => false
  }

  def get(col:Int, row:Int, cols:Int) = apply(row * cols + col)
  def set(col:Int, row:Int, value:Int, cols:Int) = update(row * cols + col, value) 

  def getDouble(col:Int, row:Int, cols:Int) = applyDouble(row * cols + col)
  def setDouble(col:Int, row:Int, value:Double, cols:Int) = updateDouble(row * cols + col, value) 
  
  def asArray = this
}


// TODO: move update to only exist in StrictRasterData
trait StrictRasterData extends ArrayRasterData with Serializable {
  override def copy():StrictRasterData
  def force = this
}

// TODO: also extend serializable?
trait LazyRasterData extends ArrayRasterData {
  def update(i:Int, x:Int) = sys.error("immutable")
  def force:StrictRasterData = {
    val len = length
    val d = alloc(len)
    var i = 0
    while (i < len) {
      d(i) = apply(i)
      i += 1
    }
    d
  }
}

// strict implementations follow

/**
 * RasterData based on Array[Int] (each cell as an Int).
 */
object IntArrayRasterData {
  def apply(array:Array[Int]) = new IntArrayRasterData(array)
  def ofDim(size:Int) = new IntArrayRasterData(Array.ofDim[Int](size))
  def empty(size:Int) = new IntArrayRasterData(Array.fill[Int](size)(NODATA))
}

final class IntArrayRasterData(array:Array[Int]) extends StrictRasterData {
  def getType = TypeInt
  def alloc(size:Int) = IntArrayRasterData.ofDim(size)
  def length = array.length
  def apply(i:Int) = array(i)
  def update(i:Int, x: Int): Unit = array(i) = x
  def copy = IntArrayRasterData(array.clone)
  def toArray = array
}

/**
 * RasterData based on an Array[Byte] as a bitmask; values are 0 and 1.
 * Thus, there are 8 boolean (0/1) values per byte in the array. For example,
 * Array(11, 9) corresponds to (0 0 0 0 1 0 1 1), (0 0 0 0 1 0 0 1) which
 * means that we have 5 cells set to 1 and 11 cells set to 0.
 *
 * Note that unlike the other array-based raster data objects we need to be
 * explicitly told our size, since length=7 and length=8 will both need to
 * allocate an Array[Byte] with length=1.
 */
object BitArrayRasterData {
  def apply(array:Array[Byte], size:Int) = new BitArrayRasterData(array, size)
  def ofDim(size:Int) = new BitArrayRasterData(Array.ofDim[Byte]((size + 7) / 8), size)
  def empty(size:Int) = ofDim(size)
}

final class BitArrayRasterData(array:Array[Byte], size:Int) extends StrictRasterData {
  // i >> 3 is the same as i / 8 but faster
  // i & 7 is the same as i % 8 but faster
  // i & 1 is the same as i % 2 but faster
  // ~3 -> -4, that is 00000011 -> 11111100
  // 3 | 9 -> 11, that is 00000011 | 00001001 -> 00001011
  // 3 & 9 -> 1,  that is 00000011 & 00001001 -> 00000001
  // 3 ^ 9 -> 10, that is 00000011 ^ 00001001 -> 00001010
  assert(array.length == (size + 7) / 8)
  def getType = TypeBit
  def alloc(size:Int) = BitArrayRasterData.ofDim(size)
  def length = size
  def apply(i:Int) = ((array(i >> 3) >> (i & 7)) & 1).asInstanceOf[Int]
  def update(i:Int, x:Int): Unit = {
    val div = i >> 3 
    if ((x & 1) == 0) {
      // unset the nth bit
      array(div) = (array(div) & ~(1 << (i & 7))).toByte
    } else {
      // set the nth bit
      array(div) = (array(div) | (1 << (i & 7))).toByte
    }
  }
  def copy = BitArrayRasterData(array.clone, size)
  def toArray = {
    val len = size
    val arr = Array.ofDim[Int](len)
    var i = 0
    while (i < len) {
      arr(i) = apply(i)
      i += 1
    }
    arr
  }

  override def map(f:Int => Int) = {
    val f0 = f(0) & 1
    val f1 = f(1) & 1
    val arr = if (f0 == 0 && f1 == 0) {
      // array of all zeros
      Array.ofDim[Byte](array.length)
    } else if (f0 == 1 && f1 == 1) {
      // array of all ones
      Array.fill[Byte](array.length)(-1.asInstanceOf[Byte])
    } else if (f0 == 0 && f1 == 1) {
      // same data as we have now
      array.clone
    } else {
      // inverse (complement) of what we have now
      val arr = array.clone
      val len = array.length
      var i = 0
      while (i < len) {
        arr(i) = (~arr(i)).asInstanceOf[Byte]
        i += 1
      }
      arr
    }
    BitArrayRasterData(arr, size)
  }

  override def mapIfSet(f:Int => Int) = map(f)
}

/**
 * RasterData based on Array[Byte] (each cell as a Byte).
 */
object ByteArrayRasterData {
  def apply(array:Array[Byte]) = new ByteArrayRasterData(array)
  def ofDim(size:Int) = new ByteArrayRasterData(Array.ofDim[Byte](size))
  def empty(size:Int) = new ByteArrayRasterData(Array.fill[Byte](size)(Byte.MinValue))
}

final class ByteArrayRasterData(array:Array[Byte]) extends StrictRasterData {
  final val nd = Byte.MinValue
  def getType = TypeByte
  def alloc(size:Int) = ByteArrayRasterData.ofDim(size)
  def length = array.length
  def apply(i:Int) = {
    val z = array(i)
    if (z == nd) NODATA else z
  }
  def update(i:Int, x: Int) {
    array(i) = if (x == NODATA) nd else x.asInstanceOf[Byte]
  }
  def copy = ByteArrayRasterData(array.clone)
  def toArray = array.map(_.asInstanceOf[Int])

  override def mapIfSet(f:Int => Int) = {
    val arr = array.clone
    var i = 0
    val len = length
    while (i < len) {
      val z = arr(i)
      if (z != nd) arr(i) = f(z).asInstanceOf[Byte]
      i += 1
    }
    ByteArrayRasterData(arr)
  }
}

/**
 * RasterData based on Array[Short] (each cell as a Short).
 */
object ShortArrayRasterData {
  def apply(array:Array[Short]) = new ShortArrayRasterData(array)
  def ofDim(size:Int) = new ShortArrayRasterData(Array.ofDim[Short](size))
  def empty(size:Int) = new ShortArrayRasterData(Array.fill[Short](size)(Short.MinValue))
}

final class ShortArrayRasterData(array:Array[Short]) extends StrictRasterData {
  final val nd = Byte.MinValue
  def getType = TypeShort
  def alloc(size:Int) = ShortArrayRasterData.ofDim(size)
  def length = array.length
  def apply(i:Int) = {
    val z = array(i)
    if (z == nd) NODATA else z
  }
  def update(i:Int, x: Int) {
    array(i) = if (x == NODATA) nd else x.asInstanceOf[Short]
  }
  def copy = ShortArrayRasterData(array.clone)
  def toArray = array.map(_.asInstanceOf[Int])

  override def mapIfSet(f:Int => Int) = {
    val arr = array.clone
    var i = 0
    val len = length
    while (i < len) {
      val z = arr(i)
      if (z != nd) arr(i) = f(z).asInstanceOf[Short]
      i += 1
    }
    ShortArrayRasterData(arr)
  }
}

// floating-point rasters

/**
 * RasterData based on Array[Float] (each cell as a Float).
 */
object FloatArrayRasterData {
  def apply(array:Array[Float]) = new FloatArrayRasterData(array)
  def ofDim(size:Int) = new FloatArrayRasterData(Array.ofDim[Float](size))
  def empty(size:Int) = new FloatArrayRasterData(Array.fill[Float](size)(Float.NaN))
}

final class FloatArrayRasterData(array:Array[Float]) extends StrictRasterData {
  def getType = TypeFloat
  def alloc(size:Int) = FloatArrayRasterData.ofDim(size)
  def length = array.length
  def apply(i:Int) = {
    val z = array(i)
    if (java.lang.Float.isNaN(z)) NODATA else z.toInt
  }
  def update(i:Int, x:Int) {
    array(i) = if (x == NODATA) Float.NaN else x.toFloat
  }
  def copy = FloatArrayRasterData(array.clone)
  def toArray = {
    val arr = Array.ofDim[Int](length)
    var i = 0
    val len = length
    while (i < len) {
      arr(i) = array(i).toInt
      i += 1
    }
    arr
  }

  override def map(f:Int => Int) = {
    val arr = Array.ofDim[Int](length)
    var i = 0
    val len = length
    while (i < len) {
      arr(i) = f(apply(i))
      i += 1
    }
    IntArrayRasterData(arr)
  }

  override def mapIfSet(f:Int => Int) = {
    val arr = Array.fill(length)(NODATA)
    var i = 0
    val len = length
    while (i < len) {
      val z = array(i)
      if (!java.lang.Float.isNaN(z)) arr(i) = f(z.toInt)
      i += 1
    }
    IntArrayRasterData(arr)
  }

  override def applyDouble(i:Int) = array(i).toDouble
  override def updateDouble(i:Int, x:Double) = array(i) = x.toFloat
  override def toArrayDouble = array.map(_.toDouble)

  override def foreachDouble(f:Double => Unit):Unit = {
    var i = 0
    val len = length
    while(i < len) {
      f(array(i))
      i += 1
    }
  }

  override def mapDouble(f:Double => Double) = {
    val data = copy()
    var i = 0
    val len = length
    while (i < len) {
      data.updateDouble(i, f(data.applyDouble(i)))
      i += 1
    }
    data
  }

  override def mapIfSetDouble(f:Double => Double) = {
    val arr = array.clone
    var i = 0
    val len = length
    while (i < len) {
      val z = arr(i)
      if (!java.lang.Float.isNaN(z)) arr(i) = f(z).toFloat
      i += 1
    }
    FloatArrayRasterData(arr)
  }
}


/**
 * RasterData based on Array[Double] (each cell as a Double).
 */
object DoubleArrayRasterData {
  def apply(array:Array[Double]) = new DoubleArrayRasterData(array)
  def ofDim(size:Int) = new DoubleArrayRasterData(Array.ofDim[Double](size))
  def empty(size:Int) = new DoubleArrayRasterData(Array.fill[Double](size)(Double.NaN))
}

final class DoubleArrayRasterData(array:Array[Double]) extends StrictRasterData {
  def getType = TypeDouble
  def alloc(size:Int) = DoubleArrayRasterData.ofDim(size)
  def length = array.length
  def apply(i:Int) = {
    val z = array(i)
    if (java.lang.Double.isNaN(z)) NODATA else z.toInt
  }
  def update(i:Int, x:Int) {
    array(i) = if (x == NODATA) Double.NaN else x.toDouble
  }
  def copy = DoubleArrayRasterData(array.clone)
  def toArray = {
    val arr = Array.ofDim[Int](length)
    var i = 0
    val len = length
    while (i < len) {
      arr(i) = apply(i)
      i += 1
    }
    arr
  }

  override def map(f:Int => Int) = {
    val arr = Array.ofDim[Int](length)
    var i = 0
    val len = length
    while (i < len) {
      arr(i) = f(apply(i))
      i += 1
    }
    IntArrayRasterData(arr)
  }

  override def mapIfSet(f:Int => Int) = {
    val arr = Array.fill(length)(NODATA)
    var i = 0
    val len = length
    while (i < len) {
      val z = array(i)
      if (!java.lang.Double.isNaN(z)) arr(i) = f(z.toInt)
      i += 1
    }
    IntArrayRasterData(arr)
  }

  override def applyDouble(i:Int) = array(i)
  override def updateDouble(i:Int, x:Double) = array(i) = x
  override def toArrayDouble = array

  override def foreachDouble(f:Double => Unit):Unit = {
    var i = 0
    val len = length
    while(i < len) {
      f(array(i))
      i += 1
    }
  }

  override def mapDouble(f:Double => Double) = {
    val data = copy()
    var i = 0
    val len = length
    while (i < len) {
      data.updateDouble(i, f(data.applyDouble(i)))
      i += 1
    }
    data
  }

  override def mapIfSetDouble(f:Double => Double) = {
    val arr = array.clone
    var i = 0
    val len = length
    while (i < len) {
      val z = arr(i)
      if (!java.lang.Double.isNaN(z)) arr(i) = f(z)
      i += 1
    }
    DoubleArrayRasterData(arr)
  }

  override def combineDouble2(other:RasterData)(f:(Double,Double) => Double) = {
    val output = this.copy
    var i = 0
    val len = length
    while (i < len) {
      output.updateDouble(i, f(applyDouble(i), output.applyDouble(i)))
      i += 1
    }
    output
  }
}


// lazy implementations follow
trait LazyWrapper extends LazyRasterData {
  protected[this] def underlying:ArrayRasterData
  final def getType = underlying.getType
  final def alloc(size:Int) = underlying.alloc(size)
  final def length = underlying.length
  final def apply(i:Int) = underlying(i)
  final def copy = this
}

//REVIEW: create abstract Lazy classes for non-array implementations to share?
/**
 * This class is a lazy wrapper for any RasterData. It's only function is to
 * defer functions like map/mapIfSet/combine2 to produce other lazy instances.
 */
final class LazyArrayWrapper(data:ArrayRasterData) extends LazyWrapper {
  def underlying = data
  def toArray = data.toArray

  override def foreach(f:Int => Unit) = data.foreach(f)
  override def map(f:Int => Int) = LazyMap(data, f)
  override def mapIfSet(f:Int => Int) = LazyMapIfSet(data, f)
  override def combine2(other:RasterData)(f:(Int, Int) => Int) = 
    // If other is an ArrayRasterData, we know how to combine.
    // If not, we expect other to know how to combine with ArrayRasterData.
    other match {
      case a:ArrayRasterData => LazyCombine2(data, a, f)
      case o => o.combine2(data)((z2,z1) => f(z1,z2))
    }
}

object LazyArrayWrapper {
  def apply(data:ArrayRasterData):LazyRasterData = data match {
    case d:LazyRasterData => d
    case d => new LazyArrayWrapper(d)
  }
}

final class LazyMap(data:ArrayRasterData, g:Int => Int) extends LazyWrapper {
  def underlying = data
  def toArray = data.toArray.map(g)

  override def foreach(f:Int => Unit) = data.foreach(z => f(g(z)))
  override def map(f:Int => Int) = LazyMap(data, z => f(g(z)))
  override def mapIfSet(f:Int => Int) = LazyMapIfSet(this, f)
  override def combine2(other:RasterData)(f:(Int, Int) => Int) = other match {
    case a:ArrayRasterData => LazyCombine2(data, a, (z1, z2) => f(g(z1), z2))
    case o => o.combine2(data)((z2, z1) => f(g(z1), z2))
  }
}

object LazyMap {
  def apply(data:ArrayRasterData, g:Int => Int) = new LazyMap(data, g)
}

/**
 * g is the function that we will call on all cells that have data, and g0 is
 * the value that we will map all NODATA cells to.
 */
final class LazyMapIfSet(data:ArrayRasterData, g:Int => Int) extends LazyWrapper {
  def underlying = data
  def toArray = data.toArray.map(gIfSet)

  def gIfSet(z:Int) = if (z == NODATA) NODATA else g(z)

  override def foreach(f:Int => Unit) = data.foreach(z => f(gIfSet(z)))
  override def map(f:Int => Int) = LazyMap(data, z => f(gIfSet(z)))
  override def mapIfSet(f:Int => Int) = LazyMapIfSet(data, z => f(g(z)))
  override def combine2(other:RasterData)(f:(Int, Int) => Int) = other match {
    case a:ArrayRasterData => LazyCombine2(data, a, (z1, z2) => f(gIfSet(z1), z2))
    case o => o.combine2(data)((z2,z1) => f(gIfSet(z1), z2))
  }
}

object LazyMapIfSet {
  def apply(data:ArrayRasterData, f:Int => Int) = new LazyMapIfSet(data, f)
}

final class LazyCombine2(data1:ArrayRasterData,
                         data2:ArrayRasterData, g:(Int, Int) => Int) extends LazyRasterData {
  def getType = RasterData.largestType(data1, data2)
  def alloc(size:Int) = RasterData.largestAlloc(data1, data2, size)
  def length = data1.length
  def apply(i:Int) = g(data1(i), data2(i))
  def copy = this
  def toArray = {
    val len = length
    val arr = Array.ofDim[Int](len)
    var i = 0
    while (i < len) {
      arr(i) = g(data1(i), data2(i))
      i += 1
    }
    arr
  }

  override def foreach(f:Int => Unit) = {
    var i = 0
    val len = length
    while (i < len) {
      val z1 = data1(i)
      val z2 = data2(i)
      f(g(z1, z2))
      i += 1
    }
  }
  override def map(f:Int => Int) = {
    LazyCombine2(data1, data2, (a, b) => f(g(a, b)))
  }
  override def mapIfSet(f:Int => Int) = {
    LazyCombine2(data1, data2, (a, b) => { val z = g(a, b); if (z != NODATA) f(z) else NODATA })
  }
  def combine2(other:ArrayRasterData)(f:(Int, Int) => Int) = {
    // TODO: benchmark
    // maybe could implement LazyArrayCombine (like MultiLocal) to speed up
    LazyCombine2(this, other, f)
  }
}

object LazyCombine2 {
  def apply(data1:ArrayRasterData, data2:ArrayRasterData, g:(Int, Int) => Int) = {
    new LazyCombine2(data1, data2, g)
  }
}
