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
package optimus.logging

import java.io.OutputStream
import java.nio.charset.Charset
import java.nio.charset.CoderResult
import java.nio.charset.CodingErrorAction

import scala.annotation.tailrec

sealed abstract class EncoderBuffer {
  def isEmpty: Boolean = text.length() == 0 && end == 0

  val text = new java.lang.StringBuilder

  def writeTo(stream: OutputStream): Unit = {
    if (end != 0) {
      stream.write(bytes, 0, end)
      end = 0
    }
    writeText(stream)
    text.setLength(0)
  }
  protected def writeText(stream: OutputStream): Unit
  protected var bytes = new Array[Byte](1024)
  protected var end: Int = 0
}

object EncoderBuffer {
  def apply(encoder: FastEncoder) = encoder.getCharset match {
    case null => new RawEncoderBuffer()
    case cs   => new CharsetEncoderBuffer(cs)
  }
  private final class RawEncoderBuffer extends EncoderBuffer {
    private val chars = new Array[Char](1024)
    protected def writeText(stream: OutputStream): Unit = {
      val end = text.length()
      var start = 0
      while (start < end) {
        val toRead = Math.min(end - start, chars.length)
        text.getChars(start, start + toRead, chars, 0)
        var index = 0
        while (index < toRead) {
          bytes(index) = chars(index).toByte
          index += 1
        }
        stream.write(bytes, 0, toRead)
        start += toRead
      }
    }
  }
  private final class CharsetEncoderBuffer(val cs: Charset) extends EncoderBuffer {
    import java.nio._
    def blockSize = 1024
    val encoder = cs.newEncoder()
    encoder.onMalformedInput(CodingErrorAction.IGNORE)
    encoder.onMalformedInput(CodingErrorAction.IGNORE)
    val bb: ByteBuffer = ByteBuffer.allocate((blockSize * encoder.maxBytesPerChar()).toInt)
    val cb: CharBuffer = CharBuffer.allocate(blockSize)

    override protected def writeText(stream: OutputStream): Unit = {
      encoder.reset()
      var start = 0
      var end = blockSize
      val max = text.length()
      while (end < max) {
        cb.position(0)
        bb.position(0)
        text.getChars(start, end, cb.array(), 0)

        encoder.encode(cb, bb, false)
        stream.write(bb.array(), 0, bb.position())

        start = end
        end += blockSize
      }
      cb.position(0)
      bb.position(0)
      text.getChars(start, max, cb.array(), 0)
      encoder.encode(cb, bb, true)
      encoder.flush(bb)
      stream.write(bb.array(), 0, bb.position())
    }
  }
}
