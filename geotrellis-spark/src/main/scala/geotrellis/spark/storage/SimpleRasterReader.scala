package geotrellis.spark.storage

import geotrellis.spark.formats.TileIdWritable
import geotrellis.spark.formats.ArgWritable
import org.apache.hadoop.conf.Configuration
import geotrellis.raster.FloatArrayRasterData
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.MapFile
import org.apache.hadoop.fs.FileUtil
import geotrellis.spark.utils.SparkUtils
import org.apache.hadoop.fs.FileStatus
import java.io.Closeable
import geotrellis.spark.rdd.TileIdPartitioner

/* 
 * An Iterable-based reader. Note that even though Iterables have a rich set of methods
 * this implementation currently would iterate through all tiles to perform any of the  
 * operations. So for example last() would iterate through all tiles to find the last  
 * tile. Clearly, this can be optimized further in at least two ways:
 * 
 * 1. Point looksups on tile ids. Internally seeks the MapFile.Reader to the correct 
 * location for fast performance
 * 2. Range lookups on ranges of tiles (with optional start/end values). Internally
 * seeks the MapFile.Reader to the start location and stops past the end of the 
 * user-provided range
 * 
 */ 
case class SimpleRasterReader(raster: Path, conf: Configuration)
  extends Iterable[(TileIdWritable, ArgWritable)]
  with Closeable {

  def close = iterator.close

  def iterator = new Iterator[(TileIdWritable, ArgWritable)] with Closeable {

    private val curKey: TileIdWritable = new TileIdWritable
    private val curValue: ArgWritable = new ArgWritable
    private var curPartition: Int = 0

    // initialize readers and partitioner
    private val readers = getReaders
    
    def close = readers.foreach(r => if (r != null) r.close)
    
    override def hasNext = {
      if (curPartition >= readers.length)
        false
      else if (readers(curPartition).next(curKey, curValue))
        true
      else {
        curPartition += 1
        hasNext
      }
    }
    
    override def next = (curKey,curValue)

    private def getReaders: Array[MapFile.Reader] = {
      val fs = raster.getFileSystem(conf)
      val dirs = FileUtil.stat2Paths(fs.listStatus(raster)).sortBy(_.toUri.toString)

      def isData(fst: FileStatus) = fst.getPath.getName.equals("data")

      def isMapFileDir(path: Path) = fs.listStatus(path).find(isData(_)) match {
        case Some(f) => true
        case None    => false
      }

      val readers = for {
        dir <- dirs
        if (isMapFileDir(dir))
      } yield new MapFile.Reader(fs, dir.toUri().toString(), conf)

      readers
    }

  }
}

// TODO - replace with test
object SimpleRasterReader {

  def main(args: Array[String]): Unit = {
    val raster = new Path("hdfs://localhost:9000/geotrellis/images/testcostdistance-gt-ingest/10")
    val conf = SparkUtils.createHadoopConfiguration
    val reader = SimpleRasterReader(raster, conf)
    var count = 0
    reader.foreach{ case(tw,aw) => {
      println(s"tileId=${tw.get}")
      count += 1
    } } 
    //val (tw,aw) = reader.last
    //println(s"last tile id = ${tw.get}")
    reader.close
    println(s"Got $count records")
  }
}