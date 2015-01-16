package geotrellis.raster

import java.io.ByteArrayOutputStream

import java.util.zip.{Deflater, Inflater}

object Zipper {

  def apply(in: Array[Byte]): Array[Byte] = {
    val compressor = new Deflater
    compressor.setLevel(Deflater.BEST_COMPRESSION)
    compressor.setInput(in)
    compressor.finish

    val baos = new ByteArrayOutputStream(in.length)

    val buff = Array.ofDim[Byte](1024)
    while (!compressor.finished) {
      val c = compressor.deflate(buff)
      baos.write(buff, 0, c)
    }

    baos.close
    baos.toByteArray
  }

}

object UnZipper {

  def apply(in: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream(in.size * 2)

    val decompressor = new Inflater
    decompressor.setInput(in)

    val buf = Array.ofDim[Byte](1024)
    while (!decompressor.finished) {
      val c = decompressor.inflate(buf)
      baos.write(buf, 0, c)
    }

    decompressor.end

    baos.close
    baos.toByteArray
  }

}

object ZipCompressor extends Compressor {

  override def compress(tile: Tile): Array[Byte] = Zipper(tile.toBytes)

}

object ZipDecompressor extends Decompressor {

  override def decompress(
    in: Array[Byte],
    cellType: CellType,
    cols: Int,
    rows: Int): Tile = ArrayTile.fromBytes(UnZipper(in), cellType, cols, rows)

}

class ZipCompressedTile(tile: Tile) extends CompressedTile(
  tile,
  ZipCompressor,
  ZipDecompressor
)
