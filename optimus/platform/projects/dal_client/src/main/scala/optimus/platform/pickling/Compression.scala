/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package optimus.platform.pickling

import com.google.common.io.ByteStreams
import net.jpountz.lz4.LZ4BlockInputStream
import net.jpountz.lz4.LZ4Factory
import optimus.platform.Compressible

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

class CompressingException(val error: String) extends RuntimeException(error)

object Compression {
  val UNCOMPRESSED = 0
  val GZIP = 1
  // TODO (OPTIMUS-13429): Use adaptive compression.
  // val ADAPTIVE = 2
  val LZ4 = 3

  // TODO (OPTIMUS-13429): Magic numbers
  val MAX_UNCOMPRESSED_SIZE = 512
  val GZIP_CHUNK_SIZE = 4096

  def compressionMode(data: Array[Byte]): Int =
    if (data.length > MAX_UNCOMPRESSED_SIZE)
      GZIP
    else UNCOMPRESSED

  def compress(mode: Int, data: Array[Byte]): ImmutableByteArray = mode match {
    case UNCOMPRESSED => ImmutableByteArray(data)
    case GZIP =>
      val bout = new ByteArrayOutputStream()
      val out = new GZIPOutputStream(bout)
      try {
        out.write(data)
        out.finish()
      } finally {
        out.close()
      }
      val bytes = bout.toByteArray
      // In Java 16 GZIPOutputStream#writeHeader changed to write OS_UNKNOWN instead of 0 in the header bytes
      // (see https://github.com/openjdk/jdk/commit/e5866aa7560e1a6077a90a5902ec61de76922440), but we need to maintain
      // binary compatibility with values written in the DAL, so force the OS byte in the header back to 0
      bytes(9) = 0
      ImmutableByteArray(bytes)
    case unmatchedMode: Int =>
      throw new CompressingException("Unrecognized compression type: " + unmatchedMode)
  }

  def decompress(mode: Int, sz: Int, data: Array[Byte]): Array[Byte] = mode match {
    case UNCOMPRESSED => data
    case GZIP =>
      val result = new Array[Byte](sz)
      val inflater = new GZIPInputStream(new ByteArrayInputStream(data), GZIP_CHUNK_SIZE)
      try {
        var ct = 0
        while (ct < sz) {
          ct += inflater.read(result, ct, sz - ct)
        }
      } finally {
        inflater.close()
      }
      result
    case LZ4 =>
      val inputStream =
        new LZ4BlockInputStream(new ByteArrayInputStream(data), LZ4Factory.fastestInstance().fastDecompressor())
      try {
        ByteStreams.toByteArray(inputStream)
      } finally {
        inputStream.close()
      }
    case unmatchedMode: Int =>
      throw new CompressingException("Unrecognized uncompression type: " + unmatchedMode)
  }

  def decompressTaggedArray(stuff: Any): Array[Byte] = stuff match {
    case buf: Seq[_] =>
      val compression = buf(0).asInstanceOf[Int]
      val sz = buf(1).asInstanceOf[Int]
      val data = buf(2).asInstanceOf[ImmutableByteArray].data
      decompress(compression, sz, data)
  }

  val stringCompressible: Compressible[String] = new Compressible[String] {
    override def compress(t: String) = {
      val mode = GZIP
      val bytes = t.getBytes("UTF-8")
      val compressed = Compression.compress(mode, bytes)
      Seq(mode, bytes.length, compressed)
    }
    override def decompress(bytes: Any): String = bytes match {
      case s: String => s
      case _         => new String(decompressTaggedArray(bytes), "UTF-8")
    }
  }
}
