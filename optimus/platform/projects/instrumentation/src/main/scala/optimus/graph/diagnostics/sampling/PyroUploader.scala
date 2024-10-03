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
package optimus.graph.diagnostics.sampling

import com.google.common.hash.Hashing
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.ProfilerSource
import optimus.breadcrumbs.crumbs.LogPropertiesCrumb
import optimus.breadcrumbs.crumbs.Properties
import optimus.core.utils.HttpClientOIDC
import optimus.graph.diagnostics.sampling.SamplingProfiler.LoadData
import optimus.graph.diagnostics.sampling.TaskTracker.AppInstance
import optimus.platform.util.Log
import optimus.platform.util.Version
import optimus.utils.MiscUtils.Endoish._
import optimus.utils.OptimusStringUtils
import optimus.utils.PropertyUtils
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.entity.mime.HttpMultipartMode
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.springframework.http.MediaType

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Random

object PyroUploader extends Log {
  private val queueLength = PropertyUtils.get("pyro.queue.length", 10)
  private val randomDelayMs = PropertyUtils.get("pyro.upload.random.delay.ms", 10)
  private val tries = PropertyUtils.get("pyro.upload.retries", 1)
  private val backoffDelayMinMs: Int = PropertyUtils.get("pyro.upload.backoff.min.ms", 1000)
  private val backoffDelayMaxMs: Int = PropertyUtils.get("pyro.upload.backoff.max.ms", 60000)
  private val compress = PropertyUtils.get("pyro.upload.jfr.compress", false)
  val pyroUploadApp = "SamplingProfiler"
  private val defaultMetricMapping = "goroutines"

  // mapping is required for later versions of pyroscope (1.x) where the type of metric published falls into
  // well defined categories
  // kind of annoying to maintain if we change the custom metric names
  // see https://github.com/grafana/pyroscope/blob/main/pkg/ingester/pyroscope/ingest_adapter.go#L201
  private def getMetricType(originalMetric: String): String = {
    originalMetric match {
      case "cpu"   => "cpu"
      case "Alloc" => "alloc_space"
      case "Live"  => "live" // maybe inuse_space
      case "Lock"  => "lock_count" // maybe lock_duration
      case "Wait"  => "wall"
      case _       => defaultMetricMapping // everything else will default to goroutines (these are mostly plugin stats)
    }
  }

  private val GHz = (1000L * 1000 * 1000).toString

  private trait Queued
  private trait Payload extends Queued {
    def format: String
    def appInstance: AppInstance
    final def chainedID: ChainedID = appInstance.rootId
    def from: Long
    def until: Long
    def size: Long
    def cleanup(): Unit
    def request: HttpPost
  }
  private case object Done extends Queued

  private val hashing = Hashing.murmur3_128()
  sealed trait SegKey
  object AppKey extends SegKey { override def toString: String = "AP" }
  object EngineKey extends SegKey { override def toString: String = "EN" }

  // Approximate the full chain ID in eight bytes.
  // This is useful for publishing to destinations like pyroscope that can't handle high cardinality.
  def approxTags(prefix: SegKey, id: ChainedID): Seq[(String, String)] = {
    hashing.hashString(id.repr, StandardCharsets.UTF_8).asBytes().take(8).zipWithIndex.map { case (code, i) =>
      (f"$prefix%s${i}%02d", f"${code + 128}%03d")
    }
  }

  private def approxLabels(prefix: SegKey, id: ChainedID, quote: Boolean = false): Seq[String] = {
    val q = if (quote) "\"" else ""
    approxTags(prefix, id).map { case (k, v) =>
      s"$k=$q$v$q"
    }
  }

  def approxId(key: SegKey, id: ChainedID): String = approxLabels(key, id, true).mkString("{", ",", "}")

  def cleanPyroAppName(appName: String): String = appName.replaceAll("\\$", "")

  private def pyroServiceNameWithLabels(
      pyroLatestV: Boolean,
      metric: Option[String],
      pyroLabels: Seq[String]): String = {
    metric.fold(pyroUploadApp)(metricType => {
      if (pyroLatestV) {
        val mappedMetricToPyroDomain = getMetricType(metricType)
        s"$pyroUploadApp.$mappedMetricToPyroDomain" // => 'SamplingProfiler.inuse_objects, SamplingProfiler.cpu etc
      } else s"$pyroUploadApp.${cleanPyroAppName(metricType)}"
    }) + pyroLabels.mkString("{", ",", "}")
  }

  private def buildPyroPublishingName(pyroLatestV: Boolean, labels: Seq[String], metric: Option[String]): String = {
    val finalLabels =
      if (pyroLatestV) {
        metric
          .collect {
            case m if getMetricType(m) == defaultMetricMapping => labels :+ s"plugin=$m"
          }
          .getOrElse(labels)
      } else labels

    pyroServiceNameWithLabels(pyroLatestV, metric, finalLabels)
  }

  private def pyroLabels(
      pyroLatestVer: Boolean,
      loadData: LoadData,
      inst: AppInstance,
      canonical: Boolean,
      withExtraLabels: Boolean,
      pyroExtraLabels: Map[String, String]): Seq[String] = {

    val commonLabels = Seq(
      s"canonical=$canonical",
      s"cpuLoad=${Math.floor(loadData.cpuLoad * 10) / 10}",
      s"graphUtil=${Math.floor(loadData.graphUtilization * 10) / 10}",
      s"appId=${inst.appId}"
    ).applyIf(withExtraLabels) {
      _ ++ pyroExtraLabels.map { case (pyroLabelKey, pyroLabelValue) =>
        s"$pyroLabelKey=$pyroLabelValue"
      }.toSeq :+ s"chainedID=${inst.root}"
    }

    if (pyroLatestVer) commonLabels
    else {
      // EN/AP00 through EN/AP07 contain the eight bytes of the hash code of the chainId,
      // which allows us to narrow down the app source in pyroscope, without blowing out
      // its indices with millions of distinct uuids.
      // If there were multiple appIds active during this period, then this method will be called for each
      // of them, but with canonical=true only for the first.  This will allow searching pyroscope by appId
      // if desired, or for canonical publications across appIds.
      val appRoot = approxLabels(AppKey, inst.rootId)
      val procRoot = approxLabels(EngineKey, ChainedID.root)

      commonLabels ++ procRoot ++ appRoot
    }
  }

  def buildUri(
      pyroUrl: String,
      pyroLatestVersion: Boolean,
      loadData: LoadData,
      inst: AppInstance,
      from: Long,
      until: Long,
      format: String,
      metricTpe: Option[String],
      canonical: Boolean,
      withExtraLabels: Boolean,
      pyroExtraLabels: Map[String, String] = Map.empty): URI = {
    // construct the pyroscope labels/tags that will show as key value pairs in the 'Select Tag' drop down in pyro UI
    val labels = pyroLabels(pyroLatestVersion, loadData, inst, canonical, withExtraLabels, pyroExtraLabels)

    val pyroServiceName = buildPyroPublishingName(pyroLatestVersion, labels, metricTpe)

    val uriBuilder = new URIBuilder(pyroUrl)
      .setPathSegments("ingest")
      .addParameter("name", pyroServiceName)
      .addParameter("format", format)
      .addParameter("from", from.toString)
      .addParameter("until", until.toString)
      .addParameter("sampleRate", GHz) // samples are reported in ns

    if (!pyroLatestVersion) {
      val bytes = metricTpe.exists { t => t.startsWith("Alloc") || t.startsWith("Free") || t.startsWith("Live") }
      val doAverage = metricTpe.exists { t => t.startsWith("Live") }
      uriBuilder.applyIf(bytes)(_.addParameter("units", "bytes"))
      uriBuilder
        .applyIf(doAverage)(_.addParameter("aggregationType", "avg"))
    }

    uriBuilder.build()
  }
}

class PyroUploader(
    pyroUrl: String,
    pyroLatestVersion: Boolean = false,
    withExtraLabels: Boolean = false,
    id: Option[ChainedID] = None,
    dryRun: Boolean = false,
    nUploadThreads: Int = 1
) extends Log {
  import PyroUploader._

  def logRootKey(): Unit = {
    id match {
      case None =>
        log.info(
          s"Root keys: ${ChainedID.root}=${approxId(AppKey, ChainedID.root)} ${ChainedID.root}=${approxId(EngineKey, ChainedID.root)}")
      case Some(id) =>
        log.info(s"Root keys: $id=${approxId(AppKey, id)}")
    }
  }

  logRootKey()

  private val enqueued = new AtomicInteger(0)
  private val discarded = new AtomicInteger(0)
  private val uploaded = new AtomicInteger(0)
  private val errors = new AtomicInteger(0)
  def status: String = s"enqueued=$enqueued, uploaded=$uploaded, discarded=$discarded, errors=$errors"

  private val queue = new ArrayBlockingQueue[Queued](queueLength)
  private case class JFR(
      meta: LoadData,
      appInstance: AppInstance,
      from: Long,
      until: Long,
      path: Path,
      canonical: Boolean,
      delete: Boolean)
      extends Payload {
    override def format = "jfr"
    override def size: Long = Files.size(path)
    override def cleanup(): Unit = {
      if (delete) try {
        Files.deleteIfExists(path)
      } catch {
        case e: Exception =>
          log.error(s"Unable to delete $path: ${e.getMessage}")
      }
    }

    override def request: HttpPost = {
      val pyroUri =
        buildUri(pyroUrl, pyroLatestVersion, meta, appInstance, from, until, format, None, canonical, withExtraLabels)
      val httpPostReq = new HttpPost(pyroUri)

      httpPostReq.addHeader("Content-Type", "multipart/form-data")
      val entBuilder =
        MultipartEntityBuilder.create().setMode(HttpMultipartMode.BROWSER_COMPATIBLE).addBinaryBody("jfr", path.toFile)

      httpPostReq.setEntity(entBuilder.build())
      httpPostReq
    }
  }

  private case class Folded(
      extraLabels: Map[String, String],
      meta: LoadData,
      appInstance: AppInstance,
      from: Long,
      until: Long,
      tpe: String,
      apDump: String,
      canonical: Boolean)
      extends Payload
      with OptimusStringUtils {
    override val format = "folded"
    override def size: Long = apDump.size
    override def cleanup(): Unit = {}
    override def toString: String =
      s"Folded($appInstance, $tpe, ${Instant.ofEpochMilli(until)}, ${apDump.abbrev})"

    override def request: HttpPost = {
      val pyroUri = buildUri(
        pyroUrl,
        pyroLatestVersion,
        meta,
        appInstance,
        from,
        until,
        format,
        Some(tpe),
        canonical,
        withExtraLabels,
        extraLabels)
      val httpPostReq = new HttpPost(pyroUri)
      httpPostReq.setEntity(new StringEntity(apDump, StandardCharsets.UTF_8))
      httpPostReq.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE)
      httpPostReq.addHeader(HttpHeaders.ACCEPT, "*/*")
      httpPostReq
    }
  }

  private def enqueue(payload: Queued, block: Boolean = false): Unit = {
    if (payload ne Done)
      enqueued.incrementAndGet()
    if (block) queue.put(payload)
    else
      while (!queue.offer(payload)) {
        queue.poll() match {
          case null =>
          case discard: Payload =>
            val msg = s"PyroUploader discarding $discard"
            discarded.incrementAndGet()
            log.warn(msg)
            Breadcrumbs.error(discard.chainedID, LogPropertiesCrumb(_, ProfilerSource, Properties.logMsg -> msg))
            discard.cleanup()
          case _ =>
        }
      }
  }

  def jfr(
      meta: LoadData,
      inst: AppInstance,
      from: Long,
      until: Long,
      path: Path,
      canonical: Boolean,
      blocking: Boolean = false,
      delete: Boolean = true) = enqueue(
    JFR(
      meta = meta,
      appInstance = inst,
      from = from,
      until = until,
      path = path,
      canonical = canonical,
      delete = delete),
    blocking)
  def folded(
      meta: LoadData,
      inst: AppInstance,
      from: Long,
      until: Long,
      tpe: String,
      dump: String,
      canonical: Boolean,
      blocking: Boolean = false,
      extraLabels: Map[String, String] = Map.empty) = enqueue(
    Folded(
      extraLabels,
      meta = meta,
      appInstance = inst,
      from = from,
      until = until,
      tpe = tpe,
      apDump = dump,
      canonical = canonical),
    blocking)

  private val oidcHttpClient = HttpClientOIDC.create(nThreads = 2 * (nUploadThreads + 1))
  private def delay(ms: Long): Unit = {
    Thread.sleep(ms)
  }
  private val activeThreads = new CountDownLatch(nUploadThreads)
  private var backoffMs = backoffDelayMinMs

  for (i <- 1 to nUploadThreads) {
    val thread = new Thread {
      override def run(): Unit = {
        while (true) {
          queue.take() match {
            case Done =>
              queue.add(Done)
              activeThreads.countDown()
              return
            case p: Payload =>
              upload(p)
          }
        }
        activeThreads.countDown()
      }
    }
    thread.setDaemon(true)
    thread.setName(s"PyroUploader$i")
    thread.start()
  }

  def shutdown(timeout: Int = 100): Unit = {
    enqueue(Done)
    activeThreads.await(timeout, TimeUnit.SECONDS)
  }

  private def warn(text: String): Unit = {
    val msg = s"PyroUploader: $text"
    log.warn(msg)
    Breadcrumbs.warn(
      ChainedID.root,
      LogPropertiesCrumb(_, ProfilerSource, Properties.logMsg -> msg :: Version.properties))
  }

  private def backoff(text: String): Unit = {
    val d = backoffMs + Random.nextInt(randomDelayMs)
    warn(s"$text; delaying $d ms")
    delay(d)
    synchronized {
      backoffMs *= 2
      if (backoffMs > backoffDelayMaxMs) backoffMs = backoffDelayMaxMs
    }
  }
  private def resetBackoff(): Unit = synchronized {
    backoffMs = backoffDelayMinMs
  }

  private val payloadID = new AtomicInteger(0)

  private def upload(upload: Payload): Unit = {
    import upload._
    val id = payloadID.incrementAndGet()
    val d = Random.nextInt(randomDelayMs)
    delay(d)
    var response: CloseableHttpResponse = null
    var success = false
    var i = 0
    if (dryRun) {
      log.info(s"Dry run: $upload, size=$size")
      success = true
    } else do {
      i += 1
      try {
        log.info(s"Uploading snapshot $id $upload, size=$size")
        val postRequest = upload.request
        if (!pyroLatestVersion) {
          val getReq = new HttpGet(request.getURI.toString)
          getReq.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_HTML_VALUE)
          getReq.addHeader(HttpHeaders.ACCEPT, "*/*")
          oidcHttpClient.execute(getReq) // needed to initialize the oidc oauth
        }
        response = oidcHttpClient.execute(postRequest)
      } catch {
        case e: Exception =>
          errors.incrementAndGet()
          backoff(s"PyroUploader: Exception uploading snapshot $id $upload: ${e.getMessage}")
      } finally {
        if (response ne null) {
          val code = response.getStatusLine.getStatusCode()
          if (code < 400) {
            success = true
            uploaded.incrementAndGet()
            resetBackoff()
          } else {
            val body = response.getEntity()
            val content = Option(body).fold("<empty>")(_.toString)
            errors.incrementAndGet()
            backoff(
              s"PyroUploader: Error uploading $id snapshot code=$code status=${response.getStatusLine} $upload ($size): $content")
          }
          response.close()
          response = null
        }
      }
    } while (!success && i < tries)
    if (!success) {
      warn(s"Giving up on $id $upload")
      discarded.incrementAndGet()
    }
    cleanup()
  }
}
