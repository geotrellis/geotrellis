/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.io

import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff.reader.GeoTiffReader.GeoTiffInfo
import geotrellis.util.LazyLogging
import geotrellis.raster.GridBounds
import geotrellis.raster._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

private [geotrellis] trait GeoTiffInfoReader extends LazyLogging {
  val geoTiffInfo: List[(String, GeoTiffInfo)]
  def geoTiffInfoRdd(implicit sc: SparkContext): RDD[String]
  def getGeoTiffInfo(uri: String): GeoTiffInfo

  lazy val averagePixelSize: Option[Int] =
    if(geoTiffInfo.nonEmpty) {
      Some((geoTiffInfo.map(_._2.bandType.bytesPerSample.toLong).sum / geoTiffInfo.length).toInt)
    } else {
      logger.error(s"No tiff tags found.")
      None
    }

  def windowsCount(maxTileSize: Option[Int] = None): Int =
    geoTiffInfo
      .flatMap { case (_, info) =>
        RasterReader.listWindows(info.segmentLayout.totalCols, info.segmentLayout.totalRows, maxTileSize)
      }
      .length

  def estimatePartitionsNumber(partitionBytes: Long, maxTileSize: Option[Int] = None): Option[Int] = {
    (maxTileSize, averagePixelSize) match {
      case (Some(tileSize), Some(pixelSize)) =>
        val numPartitions = (tileSize * pixelSize * windowsCount(maxTileSize) / partitionBytes).toInt
        logger.info(s"Estimated partitions number: $numPartitions")
        if (numPartitions > 0) Some(numPartitions)
        else None
      case _ =>
        logger.error("Can't estimate partitions number")
        None
    }
  }

  def windowsByBytes(
    partitionBytes: Long,
    maxSize: Int
  )(implicit sc: SparkContext): RDD[(String, Array[GridBounds])] = {
    geoTiffInfoRdd.flatMap({ uri =>
      val md = getGeoTiffInfo(uri)
      val cols = md.segmentLayout.totalCols
      val rows = md.segmentLayout.totalRows
      val segCols = md.segmentLayout.tileLayout.tileCols
      val segRows = md.segmentLayout.tileLayout.tileRows
      val cellType = md.cellType
      val depth = {
        cellType match {
          case BitCellType | ByteCellType | UByteCellType | ByteConstantNoDataCellType | ByteUserDefinedNoDataCellType(_) | UByteConstantNoDataCellType | UByteUserDefinedNoDataCellType(_) => 1
          case ShortCellType | UShortCellType | ShortConstantNoDataCellType | ShortUserDefinedNoDataCellType(_) | UShortConstantNoDataCellType | UShortUserDefinedNoDataCellType(_) => 2
          case IntCellType | IntConstantNoDataCellType | IntUserDefinedNoDataCellType(_) => 4
          case FloatCellType | FloatConstantNoDataCellType | FloatUserDefinedNoDataCellType(_) => 4
          case DoubleCellType | DoubleConstantNoDataCellType | DoubleUserDefinedNoDataCellType(_) => 8
        }
      }
      val (tileCols, tileRows, fileWindows) =
        RasterReader.listWindows(cols, rows, maxSize, segCols, segRows)
      val windowBytes = tileCols * tileRows * depth

      var currentBytes = 0
      val currentPartition = mutable.ArrayBuffer.empty[GridBounds]
      val allPartitions = mutable.ArrayBuffer.empty[Array[GridBounds]]

      fileWindows.foreach({ gb =>
        // Add the window to the present partition
        if (currentBytes + windowBytes <= partitionBytes) {
          currentPartition.append(gb)
          currentBytes += windowBytes
        }
        // The window is small enough to fit into some partition,
        // but not this partition; start a new partition.
        else if ((currentBytes + windowBytes > partitionBytes) && (windowBytes < partitionBytes)) {
          allPartitions.append(currentPartition.toArray)
          currentPartition.clear
          currentPartition.append(gb)
          currentBytes = windowBytes
        }
        // The window is too large to fit into any partition.
        else {
          allPartitions.append(Array(gb))
        }
      })
      allPartitions.append(currentPartition.toArray)
      allPartitions.toArray.map({ array => (uri, array) })
    })
  }

  /**
    * Function calculates a split of segments, to minimize segments reads.
    *
    * Returns RDD of pairs: (URI, Array[GridBounds])
    * where GridBounds are gird bounds of a particular segment,
    * each segment can only be in a single partition.
    * */
  def segmentsByPartitionBytes(
    partitionBytes: Long = Long.MaxValue,
    maxTileSize: Option[Int] = None
  )(implicit sc: SparkContext): RDD[(String, Array[GridBounds])] = {
    geoTiffInfoRdd.flatMap { uri =>
      val bufferKey = uri

      val md = getGeoTiffInfo(uri)
      val allSegments = mutable.Set(md.segmentBytes.indices: _*)
      val allSegmentsInitialSize = allSegments.size

      val layout = md.segmentLayout
      val segmentBytes = md.segmentBytes

      // list of desired windows, we'll try to pack them with segments if its possible
      val windows = RasterReader.listWindows(layout.totalCols, layout.totalRows, maxTileSize)
      // a buffer with segments refs
      val buf: mutable.ListBuffer[(String, Array[GridBounds])] = mutable.ListBuffer()

      // walk though all desired windows
      windows.foreach { gb =>
        // buffer of segments which should fit bytes size & window size
        val windowsBuffer: mutable.ListBuffer[Array[GridBounds]] = mutable.ListBuffer() // a buffer with segments refs
        // current buffer
        val currentBuffer: mutable.ListBuffer[GridBounds] = mutable.ListBuffer()
        var currentSize = 0
        var currentBoundsLength = 0

        // go through all segments which intersect desired bounds and was not put into any partition yet
        layout.intersectingSegments(gb).intersect(allSegments.toSeq).foreach { i =>
          val segmentSize = layout.getSegmentSize(i)
          val segmentSizeBytes = segmentBytes.getSegmentByteCount(i) * md.bandCount
          val segmentGb = layout.getGridBounds(i)


          // if segment is inside the window
          if ((gb.contains(segmentGb) && layout.isTiled) || segmentSize <= gb.sizeLong && layout.isStriped) {
            // if segment fits partition
            if (segmentSizeBytes <= partitionBytes) {
              // check if we still want to put segment into the same partition
              if (currentSize <= partitionBytes && (layout.isTiled || layout.isStriped && currentBoundsLength <= gb.sizeLong)) {
                currentSize += segmentSizeBytes
                currentBuffer += segmentGb
                currentBoundsLength += segmentSize
              } else { // or put it into a separate partition
                windowsBuffer += currentBuffer.toArray
                currentBuffer.clear()
                currentSize = segmentSizeBytes
                currentBoundsLength = segmentSize
                currentBuffer += segmentGb
              }
            } else {
              // case when segment size is bigger than a desired partition size
              // it is better to use a different strategy for these purposes
              logger.warn("Segment size is bigger than a desired partition size, " +
                "though it fits the window size. You can consider a different partitioning strategy.")
              windowsBuffer += Array(segmentGb)
            }
            allSegments -= i
          }
        }

        // if we have smth left in the current buffer
        if(currentBuffer.nonEmpty) windowsBuffer += currentBuffer.toArray

        windowsBuffer.foreach { indices => buf += (bufferKey -> indices) }
      }

      // there can be left some windows
      if (allSegments.nonEmpty) {
        logger.warn(s"Some segments don't fit windows (${allSegments.size} of $allSegmentsInitialSize).")

        val windowsBuffer: mutable.ListBuffer[Array[GridBounds]] = mutable.ListBuffer() // a buffer with segments refs
        val currentBuffer: mutable.ListBuffer[GridBounds] = mutable.ListBuffer()
        val gbSize = windows.head.sizeLong
        var currentSize = 0
        var currentBoundsLength = 0

        allSegments.foreach { i =>
          val segmentSize = layout.getSegmentSize(i)
          val segmentSizeBytes = segmentBytes.getSegmentByteCount(i) * md.bandCount
          val segmentGb = layout.getGridBounds(i)

          if (currentSize <= partitionBytes) {
            if (currentSize <= partitionBytes && (layout.isTiled || layout.isStriped && currentBoundsLength <= gbSize)) {
              currentSize += segmentSizeBytes
              currentBuffer += segmentGb
              currentBoundsLength += segmentSize
            } else {
              windowsBuffer += currentBuffer.toArray
              currentBuffer.clear()
              currentSize = segmentSizeBytes
              currentBoundsLength = segmentSize
              currentBuffer += segmentGb
            }
          } else {
            // case when segment size is bigger than a desired partition size
            // it is better to use a different strategy for these purposes
            logger.warn("Segment size is bigger than a desired partition size, " +
              "and it doesn't fit window a desired window size. You can consider a different partitioning strategy.")
            windowsBuffer += Array(segmentGb)
          }
        }

        // if we have smth left in the current buffer
        if(currentBuffer.nonEmpty) windowsBuffer += currentBuffer.toArray

        windowsBuffer.foreach { indices => buf += (bufferKey -> indices) }
      }

      buf
    }
  }
}
