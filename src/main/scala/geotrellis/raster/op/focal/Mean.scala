package geotrellis.raster.op.focal

import scala.math._

import geotrellis._

case class Mean(r:Op[Raster], f:Kernel) extends Op1(r)({
  r => Result(f.handle(r, new MeanStrategy(r)))
})

protected[focal] class MeanStrategy(r:Raster) extends Strategy[Raster, MeanCell](Aggregated) {
  val d = DoubleArrayRasterData.ofDim(r.cols, r.rows)
  def store(col:Int, row:Int, cc:MeanCell) { d.setDouble(col, row, cc.get()) }
  def get() = Raster(d, r.rasterExtent)
  def makeCell() = new MeanCell
}

protected[focal] class MeanCell extends Cell[MeanCell] {
  var total:Double = 0.0
  var count:Int = 0
  def clear() { total = 0.0; count = 0 }
  def add(cc:MeanCell) { total += cc.total; count += cc.count }
  def add(col:Int, row:Int, r:Raster) { total += r.get(col, row); count += 1 }
  def remove(cc:MeanCell) { total -= cc.total; count -= cc.count }
  def remove(col:Int, row:Int, r:Raster) { total -= r.get(col, row); count -= 1 }
  def get() = total / count
}
