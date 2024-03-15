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
package optimus.utils

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.GZIPInputStream

import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdCompressCtx
import com.github.luben.zstd.ZstdDecompressCtx
import com.github.luben.zstd.ZstdInputStream
import com.google.common.io.ByteStreams

object ZstdUtils {

  private[utils] val DefaultCompressLevel: Int = 3 // 3 is the default level upstream
  private[utils] val ZstdMagicNumber = -47205080 // stored inline to avoid native lib loading from inside isZstd

  private val CompressCtx = new ThreadLocal[ZstdCompressCtx]() {
    override def initialValue() = new ZstdCompressCtx()
  }

  private val DecompressCtx = new ThreadLocal[ZstdDecompressCtx]() {
    override def initialValue() = new ZstdDecompressCtx()
  }

  def isZstd(input: Array[Byte]) = {
    if ((input eq null) || (input.length < 4)) {
      false
    } else {
      val magicNumber = ByteBuffer.wrap(input, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt(0)
      magicNumber == ZstdMagicNumber
    }
  }

  def compress(src: Array[Byte], level: Int = DefaultCompressLevel): Array[Byte] = {
    val ctx = CompressCtx.get()
    ctx.setLevel(level)
    ctx.compress(src)
  }

  def decompress(src: Array[Byte]): Array[Byte] = {
    val outputSize = Zstd.getFrameContentSize(src).toInt

    if (outputSize > 0) {
      val ctx = DecompressCtx.get()
      ctx.decompress(src, outputSize)
    } else {
      // decompressed size is not known upfront - we need to use streaming mode
      decompressUnknownSize(src)
    }
  }

  private def decompressUnknownSize(input: Array[Byte]): Array[Byte] = {
    val bais = new ByteArrayInputStream(input)
    val zstdInputStream = new ZstdInputStream(bais)
    try {
      ByteStreams.toByteArray(zstdInputStream)
    } finally {
      zstdInputStream.close()
    }
  }

  def decompressZstdOrGzip(input: Array[Byte]): Array[Byte] = {
    if (isZstd(input)) {
      decompress(input)
    } else {
      val gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(input))
      try {
        ByteStreams.toByteArray(gzipInputStream)
      } finally {
        gzipInputStream.close()
      }
    }
  }

}
