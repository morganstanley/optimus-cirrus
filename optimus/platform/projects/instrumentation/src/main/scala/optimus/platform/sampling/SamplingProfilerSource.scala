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
package optimus.platform.sampling

import com.github.luben.zstd.RecyclingBufferPool
import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdOutputStream
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.BreadcrumbsLocalPublisher
import optimus.breadcrumbs.BreadcrumbsPublisher
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Crumb.CrumbFlag
import optimus.breadcrumbs.crumbs.Crumb.Source
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.diagnostics.sampling.SamplingProfiler
import optimus.graph.diagnostics.sampling.SamplingProfiler.MILLION
import optimus.platform.util.Log
import optimus.utils.PropertyUtils

import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Paths
import java.util.Base64
import java.util.concurrent.atomic.AtomicLong
import scala.util.control.NonFatal

class SourceCompression(zstdLevel: Int) extends Log {

  @volatile private var enabled = true
  private val buf = new ByteArrayOutputStream()
  private var uncompressedBytesInBatch = 0
  private var batchCount = 0
  private var writer: PrintStream = null

  val totalUncompressedBytes = new AtomicLong(0)

  private def createStream(): Unit = if (enabled) try {
    if (writer ne null) writer.close()
    val zstd = new ZstdOutputStream(buf, RecyclingBufferPool.INSTANCE, zstdLevel)
    zstd.setCloseFrameOnFlush(true)
    zstd.setWorkers(2)
    writer = new PrintStream(zstd)
  } catch {
    case NonFatal(e) =>
      log.error("Exception creating compression stream", e)
      enabled = false
  }

  private def getBytesAndReset(): (Int, Int, Array[Byte]) = buf.synchronized {
    if (!enabled || uncompressedBytesInBatch == 0) {
      (0, 0, Array.emptyByteArray)
    } else
      try {
        writer.flush()
        val b = buf.toByteArray
        writer.close()
        buf.reset()
        createStream()
        val size = uncompressedBytesInBatch
        val len = batchCount
        batchCount = 0
        uncompressedBytesInBatch = 0
        (len, size, b)
      } catch {
        case NonFatal(e) =>
          log.error(s"Exception extracting compressed bytes", e)
          enabled = false
          (0, 0, Array.emptyByteArray)
      }
  }
  createStream()

  // This method exists primarily so it can be captured in the samplingZstd metric.
  private def writeToZstd(s: String): Unit = buf.synchronized(writer.println(s))

  // Return true if we've incorporated the crumb into the compression buffer.  Note that we explicity
  // do not do so if the crumb is already a compressed blob.
  def incorporate(c: Crumb): Boolean = enabled && {
    c match {
      case p: PropertiesCrumb if p.contains(Properties.blob) || p.contains(Properties.crumbplexerIgnore) =>
        false
      case _ =>
        val s = c.asJSON.toString()
        try {
          writeToZstd(s)
          uncompressedBytesInBatch += s.length
          totalUncompressedBytes.addAndGet(s.length)
          batchCount += 1
          true
        } catch {
          case NonFatal(e) =>
            log.error(s"Exception incorporating crumb to compress", e)
            enabled = false
            false
        }
    }
  }

  // Write the accumulated compressed buffer and return a crumb containing the blob.
  // Captured in the samplingZstd metric.
  def compressToBlobCrumb(): Option[Crumb] = {
    val (batchCount, batchTotalSize, bytes) = getBytesAndReset()
    if (bytes.size > 0) {
      val blob = Base64.getEncoder.encodeToString(bytes)
      val crumb = PropertiesCrumb(
        ChainedID.root,
        SamplingProfilerSource,
        Properties.blob -> blob,
        Properties.blobLen -> blob.length,
        Properties.blobOrigLen -> batchTotalSize,
        Properties.blobBatch -> batchCount,
      )
      log.info(
        s"Compressed crumb: orig=$batchTotalSize compressed=${blob.size} batch=$batchCount ratio=${blob.size.toDouble / batchTotalSize}")
      Some(crumb)
    } else None
  }
}

object SamplingProfilerSource extends Source with Log {
  override val name: String = "SP"
  override val flags = Set(CrumbFlag.DoNotReplicate)
  private val localPublisher: Option[BreadcrumbsPublisher] =
    PropertyUtils.get("optimus.sampling.gzip.path").map { path =>
      new BreadcrumbsLocalPublisher(Paths.get(path))
    }

  private val compressor = PropertyUtils.get("optimus.sampling.zstd").flatMap {
    case "" | "true" => Some(new SourceCompression(Zstd.defaultCompressionLevel()))
    case "false"     => None
    case s           => Some(new SourceCompression(s.toInt))
  }

  def flush(): Unit = localPublisher.foreach(_.flush())

  def compressAndSendBlobCrumb(): Unit = for {
    c <- compressor
    crumb <- c.compressToBlobCrumb()
  } Breadcrumbs.send(crumb)

  override def adaptInternal(c: Crumb): Boolean =
    SamplingProfiler.stillPulsing() && compressor.fold(false)(_.incorporate(c))

  override val publisherOverride: Option[BreadcrumbsPublisher] = localPublisher
  override val filterable = PropertyUtils.get("optimus.sampling.profiler.filterable", default = false)

  override def countAnalysisMap: Map[String, Double] = {
    val cm = super.countAnalysisMap
    compressor.fold(cm)(c => cm ++ Map("uncompressed" -> c.totalUncompressedBytes.get.toDouble / MILLION))
  }
}
