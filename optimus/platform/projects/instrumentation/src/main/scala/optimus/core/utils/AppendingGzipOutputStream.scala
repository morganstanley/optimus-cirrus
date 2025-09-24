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

package optimus.core.utils
import one.profiler.AsyncProfiler
import optimus.graph.AsyncProfilerIntegration

import java.io.IOException
import java.io.OutputStream
import java.nio.file.Path

/**
 * An output stream that can be closed and subsequently re-opened for appending.
 * This does _not_ work with regular GZIPOutputStream.  See various StackOverflow answers by
 * Mark Adler, pointing to https://github.com/madler/zlib/blob/develop/examples/gzlog.c, which we
 * use here via JNI.
 */
class AppendingGzipOutputStream(path: Path, bufsize: Int = 1000000) extends OutputStream {

  // Since this extends OutputStream, the fundamental unit of writing is a byte, so we buffer writes until we have
  // something relatively chunky to pass to gzlog.
  assert(bufsize > 0)

  private val (inst: AsyncProfiler, gzlogPointer: Long) = {
    if (!AsyncProfilerIntegration.ensureLoadedIfEnabled())
      throw new RuntimeException("AsyncProfiler not available") //
    val inst = AsyncProfiler.getInstance()
    // gzlog is going to re-append the .gz
    val pointer: Long = inst.gzlogOpen(path.toString.stripSuffix(".gz"))
    if (pointer == 0)
      throw new IOException(s"Unable to open $path")
    (inst, pointer)
  }

  override def toString: String = s"GZLog(path=$path, gzlog=${gzlogPointer.toHexString})"

  private val writeBuffer = new Array[Byte](bufsize)
  private var i = 0

  // This is just passing the accumulated byte buffer to gzlogWrite.  It's not a file-system flush.
  // Called under lock
  private def flushWriteBuffer(): Unit = {
    if (i > 0) {
      val ret = inst.gzlogWrite(gzlogPointer, writeBuffer, 0, i)
      if (ret != 0)
        throw new IOException(s"write exception $ret for $path")
      i = 0
    }
  }

  override def write(b: Int): Unit = synchronized {
    writeBuffer(i) = b.toByte
    i += 1
    if (i == bufsize) flushWriteBuffer()
  }

  override def flush(): Unit = synchronized {
    flushWriteBuffer()
    val ret = inst.gzlogFlush(gzlogPointer)
    if (ret != 0)
      throw new IOException(s"flush exception $ret for $path")
  }

  override def close(): Unit = synchronized {
    val ret = inst.gzlogClose(gzlogPointer)
    if (ret != 0)
      throw new IOException(s"closing exception $ret for $path")
  }
}
