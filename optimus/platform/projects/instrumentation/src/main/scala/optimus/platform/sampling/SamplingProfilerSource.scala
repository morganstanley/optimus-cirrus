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

import com.github.luben.zstd.Zstd
import optimus.breadcrumbs.BreadcrumbsLocalPublisher
import optimus.breadcrumbs.BreadcrumbsPublisher
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Crumb.CrumbFlag
import optimus.graph.diagnostics.sampling.SamplingProfiler
import optimus.graph.diagnostics.sampling.SamplingProfiler.MILLION
import optimus.utils.PropertyUtils

import java.util
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong

object SamplingProfilerSource extends Crumb.Source {
  override val name: String = "SP"
  override val flags = Set(CrumbFlag.DoNotReplicate)
  private val localPublisher: Option[BreadcrumbsLocalPublisher] =
    PropertyUtils.get("optimus.sampling.gzip.path").map { path =>
      new BreadcrumbsLocalPublisher(Paths.get(path))
    }
  override val publisherOverride: Option[BreadcrumbsPublisher] = localPublisher
  override val filterable = PropertyUtils.get("optimus.sampling.profiler.filterable", default = true)

  def flush(): Unit = localPublisher.foreach(_.flush())

  private var datalen = 0
  private var bufsize = 1000 * 1000
  // Compress on every publication, or when we exceed 50MB uncompressed
  private val compressThreshold = 50 * bufsize
  private var buf = new Array[Byte](bufsize)
  private val compressedSoFar = new AtomicLong(0)

  private def compress(): Double = synchronized {
    if (datalen == 0) compressedSoFar.get().toDouble
    else {
      val maxCompressedLength = Zstd.compressBound(datalen).toInt
      val compressedBuf = new Array[Byte](maxCompressedLength)
      val compressedSize =
        Zstd.compressByteArray(compressedBuf, 0, maxCompressedLength, buf, 0, datalen, Zstd.defaultCompressionLevel())
      datalen = 0
      compressedSoFar.addAndGet(compressedSize).toDouble
    }
  }

  override def anaylyzeKafkaBlob$(s: String): Boolean = {
    if (SamplingProfiler.stillPulsing()) synchronized {
      val b = s.getBytes
      val newlen = datalen + b.size
      while (newlen > bufsize) {
        buf = util.Arrays.copyOf(buf, bufsize * 2)
        bufsize *= 2
      }
      System.arraycopy(b, 0, buf, datalen, b.size)
      datalen = newlen
    }
    if (datalen > compressThreshold) compress()
    super.anaylyzeKafkaBlob$(s)
  }

  override def countAnalysisMap: Map[String, Double] = super.countAnalysisMap ++ Map("zstd" -> compress() / MILLION)

}
