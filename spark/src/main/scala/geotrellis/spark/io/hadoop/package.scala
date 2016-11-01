package geotrellis.spark.io

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io.hadoop.formats._
import geotrellis.spark.util.KryoSerializer
import geotrellis.util.MethodExtensions
import geotrellis.vector.ProjectedExtent

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce._
import org.apache.spark.SparkContext

import scala.reflect._

package object hadoop extends Implicits {
  implicit def stringToPath(path: String): Path = new Path(path)

  implicit def spatialSinglebandGeoTiffInputFormattable = SpatialSinglebandGeoTiffInputFormattable
  implicit def spatialMultibandGeoTiffInputFormattable = SpatialMultibandGeoTiffInputFormattable
  implicit def temporalSinglebandGeoTiffInputFormattable = TemporalSinglebandGeoTiffInputFormattable
  implicit def temporalMultibandGeoTiffInputFormattable = TemporalMultibandGeoTiffInputFormattable

  implicit class withHadoopConfigurationMethods(val self: Configuration) extends MethodExtensions[Configuration] {
    def modify(f: Job => Unit): Configuration = {
      val job = Job.getInstance(self)
      f(job)
      job.getConfiguration
    }

    def withInputPath(path: Path): Configuration =
      modify(FileInputFormat.addInputPath(_, path))

    /** Creates a Configuration with all files in a directory (recursively searched)*/
    def withInputDirectory(path: Path): Configuration = {
      val allFiles = HdfsUtils.listFiles(path, self)
      if(allFiles.isEmpty) {
        sys.error(s"$path contains no files.")
      }
      HdfsUtils.putFilesInConf(allFiles.mkString(","), self)
    }

    /** Creates a configuration with a given directory, to search for all files
      * with an extension contained in the given set of extensions */
    def withInputDirectory(path: Path, extensions: Seq[String]): Configuration = {
      val searchPath = path.toString match {
        case p if extensions.exists(p.endsWith) => path
        case p =>
          val extensionsStr = extensions.mkString("{", ",", "}")
          new Path(s"$p/*$extensionsStr")
      }

      withInputDirectory(searchPath)
    }

    def setSerialized[T: ClassTag](key: String, value: T): Unit = {
      val ser = KryoSerializer.serialize(value)
      self.set(key, new String(ser.map(_.toChar)))
    }

    def getSerialized[T: ClassTag](key: String): T = {
      val s = self.get(key)
      KryoSerializer.deserialize(s.toCharArray.map(_.toByte))
    }

    def getSerializedOption[T: ClassTag](key: String): Option[T] = {
      val s = self.get(key)
      if(s == null) {
        None
      } else {
        Some(KryoSerializer.deserialize(s.toCharArray.map(_.toByte)))
      }
    }
  }
}
