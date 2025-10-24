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

import com.sun.management.OperatingSystemMXBean
import com.typesafe.config.ConfigFactory
import msjava.base.slr.internal.ServiceEnvironment
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.BreadcrumbsKafkaPublisher
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb
import optimus.breadcrumbs.crumbs.Crumb.ProfilerSource
import optimus.breadcrumbs.crumbs.Crumb.Source
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.Properties.Elems.nonEmptyElems
import optimus.breadcrumbs.crumbs.Properties._
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.AsyncProfilerIntegration
import optimus.graph.DiagnosticSettings
import optimus.graph.Exceptions
import optimus.graph.diagnostics.ap.StackAnalysis
import optimus.graph.diagnostics.sampling.SamplingProfiler.SamplerTrait
import optimus.graph.diagnostics.sampling.SamplingProfilerLogger.log
import optimus.graph.diagnostics.sampling.SamplingProfilerLogger.logProfireUrl
import optimus.graph.diagnostics.sampling.TaskTracker.AppInstance
import optimus.platform.sampling.SamplingProfilerSource
import optimus.platform.util.InfoDump
import optimus.platform.util.ServiceLoaderUtils
import optimus.platform.util.Version
import optimus.utils.MacroUtils.SourceLocation
import optimus.utils.MiscUtils.ThenSome
import optimus.utils.PropertyUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.lang.management.ManagementFactory
import java.time.Instant
import java.util
import java.util.Objects
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.control.NonFatal

trait SamplerProvider {
  def provide(sp: SamplingProfiler): Seq[SamplerTrait[_, _]]
  val priority: Int = Int.MaxValue
}

object SamplingProfilerConfig {
  private val config = ConfigFactory.parseResources("main/internal/samplingProfiler.conf")
  def string(key: String): String = if (config.hasPath(key)) config.getString(key) else ""
}

object SamplingProfilerLogger {
  lazy val pid = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)
  val logPrefix = s"[${SamplingProfiler.Name}$pid-${ChainedID.root}] "
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  object log {
    def debug(msg: String): Unit = logger.debug(logPrefix + msg)
    def info(msg: String): Unit = logger.info(logPrefix + msg)
    def warn(msg: String): Unit = logger.warn(logPrefix + msg)
    def warn(msg: String, t: Throwable): Unit = logger.warn(logPrefix + msg, t)
    def error(msg: String): Unit = logger.error(logPrefix + msg)
    def error(msg: String, t: Throwable): Unit = logger.error(logPrefix + msg, t)
  }
  def logProfireUrl(uuid: String): Option[String] = if (uuid.nonEmpty) {
    val urlRoot = Breadcrumbs.getEnv().flatMap {
      case qa @ (ServiceEnvironment.qa | ServiceEnvironment.uat) => Some(SamplingProfilerConfig.string("profire-qa"))
      case ServiceEnvironment.prod                               => Some(SamplingProfilerConfig.string("profire-prod"))
      case _                                                     => None
    }
    urlRoot match {
      case Some(root) =>
        val url = root + s"&pfrInput=$uuid"
        log.info(s"You can check the profiling data here: $url")
        Some(url)
      case None =>
        log.debug("Profire URL root not configured; skip logging URL.")
        None
    }
  } else None
}
import optimus.platform.breadcrumbs
object ForensicSource extends breadcrumbs.ForensicSource

trait SampleCrumbConsumer {
  def consume(id: ChainedID, source: Crumb.Source, elems: Elems): Unit
  final def consume(id: ChainedID, elems: Elems): Unit = consume(id, SamplingProfilerSource, elems)
  def flush(): Unit = {}
  def errors: Int = 0
  def sent: Int = 0

  def resetStats(): Unit = {}
}

object NullSampleCrumbConsumer extends SampleCrumbConsumer {
  override def consume(id: ChainedID, source: Crumb.Source, elems: Elems): Unit = {}
}

object DefaultSampleCrumbConsumer extends SampleCrumbConsumer {
  private val crumbsSent = new AtomicInteger(0)
  private val crumbErrors = new AtomicInteger(0)

  override def consume(id: ChainedID, source: Crumb.Source, elems: Elems): Unit = {
    val success = Breadcrumbs.info(id, PropertiesCrumb(_, source, elems))
    if (success) crumbsSent.incrementAndGet() else crumbErrors.incrementAndGet()
  }

  override def flush(): Unit = Breadcrumbs.flush()

  override def errors: Int = crumbErrors.get
  override def sent: Int = crumbsSent.get

  override def resetStats(): Unit = {
    crumbsSent.set(0)
    crumbErrors.set(0)
  }
}

class SamplingProfiler private[diagnostics] (
    private val ownerId: ChainedID,
    crumbConsumer: SampleCrumbConsumer = DefaultSampleCrumbConsumer,
    properties: Map[String, String] = Map.empty
) extends SampleCrumbConsumer {
  import SamplingProfiler._
  private val sp = this

  val startTime: Long = System.currentTimeMillis()

  override def consume(id: ChainedID, source: Crumb.Source, elems: Elems): Unit =
    crumbConsumer.consume(id, source, elems)
  override def flush(): Unit = crumbConsumer.flush()

  private val schedulerCounters = ServiceLoaderUtils.all[SchedulerCounter]

  private val stackSamplers = ServiceLoaderUtils.all[StackSampler]

  private val samplerProviders = ServiceLoaderUtils.all[SamplerProvider].sortBy(_.priority)

  val propertyUtils = new PropertyUtils(properties)

  private val pauseInactive = propertyUtils.get("optimus.sampling.pause.inactive", false)
  val periodSec = propertyUtils.get(SamplingIntervalSec, DefaultSamplingIntervalSec)
  private val spoofRoot = propertyUtils.get("optimus.sampling.rootids", true)
  private val cleanupWaitMs = propertyUtils.get("optimus.sampling.wait.ms", 5000)

  // Give the first publication a freebie.
  private val activityMeritingPublication = new AtomicInteger(1)

  private[sampling] def warn(msg: String, t: Throwable = null, counter: AtomicInteger = GlobalMaxWarnings): Unit = {
    log.warn(msg, t)
    if (Objects.isNull(counter) || counter.getAndUpdate(i => if (i > 0) i - 1 else 0) > 0) {
      Breadcrumbs.warn(
        sp.ownerId,
        PropertiesCrumb(
          _,
          ForensicSource,
          logMsg -> s"SamplingProfiler warning: $msg " ::
            severity -> "Warn" ::
            stackTrace.nonNull(Exceptions.minimizeTrace(t, 10, 3)) :: Version.properties
        )
      )
    }
  }

  private[sampling] def info(msg: String): Unit = {
    log.info(msg)
    Breadcrumbs.info(
      sp.ownerId,
      PropertiesCrumb(
        _,
        ProfilerSource,
        logMsg -> s"SamplingProfiler: $msg" :: severity -> "Info" :: Version.properties)
    )
  }

  def recordActivity(): Unit = activityMeritingPublication.incrementAndGet()

  // warn log level for higher verbosity
  warn(s"Starting SamplingProfiler at ${System
      .currentTimeMillis() - ManagementFactory.getRuntimeMXBean().getStartTime()}ms ownerId=$ownerId period=$periodSec")

  // called for side effects
  private val pulserInstance = "SP" + numPulsers.incrementAndGet()

  private val samplingPeriodMs = 1000L * periodSec
  private val waiter = new Object

  // The following are guarded by waiter
  private var _currentSnapTime: Long = 0
  @volatile private var _stillPulsing = DiagnosticSettings.samplingProfilerStatic
  private var _snapAndPublishImmediately = false

  def currentSnapTime = _currentSnapTime
  private var _previousSnapTime: Long = loadTime
  def previousSnapTime = _previousSnapTime

  private var _actualPeriodMs: Long = 0
  def actualPeriodMs: Long = _actualPeriodMs
  private var _nSnaps = 0
  def nSnaps = _nSnaps

  private var samplers: Seq[SamplerTrait[_, _]] = Seq.empty
  private val samplersLock = new Object // someday, we may support dynamic additions

  private def defaultAppInstance = AppInstance(ownerId.base, initialAppId)

  private var _publishAppInstances: Seq[AppInstance] = Seq()

  // For samplers that publish to somewhere else, e.g. task publisher app.
  def publishAppInstances: Seq[AppInstance] = _publishAppInstances
  def publishRootIds: Seq[ChainedID] = publishAppInstances.map(_.rootId)

  private var _snappedMeta: LoadData = LoadData(0.0, 0.0)
  private[sampling] def snappedMeta: LoadData = _snappedMeta

  @volatile private var hasPublished = false
  private val diagnosticsTimer = new Timer(true)
  private def scheduleDiagnosticsTask(delay: Long): Unit = {
    diagnosticsTimer.schedule(
      new TimerTask {
        override def run(): Unit = if (enabled) {
          val nextCheck =
            if (hasPublished) delay
            else {
              val msg =
                s"SamplingProfiler $ownerId has not snapped in ${System.currentTimeMillis() - _currentSnapTime} ms"
              warn(msg)
              InfoDump.dump("sampling", msg :: Nil)
              delay * 2
            }
          hasPublished = false
          scheduleDiagnosticsTask(nextCheck)
        }
      },
      delay
    )
  }
  scheduleDiagnosticsTask(propertyUtils.getLong("optimus.sampling.diagnostics.dump.ms", 5 * samplingPeriodMs))

  {
    // All metrics:
    val ss = ArrayBuffer.empty[SamplerTrait[_, _]]
    _snappedMeta = LoadData(cpuLoad = 0.0, graphUtilization = 0.0)

    // These samplers should occur first, so _publishAppInstances and _snappedMetaData are available to later ones.
    ss += new Sampler[TaskTracker.Cycle, TaskTracker.Cycle](
      sp,
      "task_cycle",
      snapper = (_: Boolean) => {
        val cycle = TaskTracker.cycle()
        // Expose outside of this sampler, so we can publish to these roots.
        _publishAppInstances = if (cycle.appInstances.isEmpty) Seq(defaultAppInstance) else cycle.appInstances
        _snappedMeta = _snappedMeta.copy(cpuLoad = osBean.getProcessCpuLoad)
        cycle
      },
      process = LATEST,
      publish = (cycle: TaskTracker.Cycle) => {
        val relevantApIds =
          if (cycle.appInstances.isEmpty && !initialAppId.isEmpty) Seq(initialAppId)
          else cycle.appInstances.map(_.appId)
        Elems(
          distActiveTime -> cycle.timeAnyActiveInCycle / NANOSPERMILLI,
          currentlyRunning -> cycle.activeChainedIDs,
          activeRoots -> cycle.appInstances.map(_.root),
          engineRootsCount -> 1,
          activeScopes -> cycle.activeScopes,
          distTasksRcvd -> cycle.numActivated,
          distTasksComplete -> cycle.numDeactivated,
          distActive -> cycle.activeInCycle.size,
          appIds -> relevantApIds,
          distLostTasks -> cycle.numLost,
          distTaskDuration -> cycle.avgDuration / NANOSPERMILLI,
        )
      }
    )
    val util = new Util(this)
    import util._
    ss += SnapNonZero(_ => crumbConsumer.errors, numCrumbFailures)
    ss += SnapNonZero(_ => Breadcrumbs.queueLength, crumbQueueLength)

    // This sampler might get disabled if there are in fact no properly initialized schedulers.
    ss += new Sampler[SchedulerCounts, SchedulerCounts](
      sp,
      "scheduler_counts",
      snapper = (_: Boolean) => {
        val counts = schedulerCounters.map(_.getCounts).foldLeft(SchedulerCounts.empty)((l, r) => l + r)
        _snappedMeta = _snappedMeta.copy(graphUtilization = counts.working.toDouble / numThreads)
        counts
      },
      process = LATEST,
      publish = (c: SchedulerCounts) =>
        Elems(
          graphUtil -> c.working.toDouble / numThreads,
          profWaitThreads -> c.waiting,
          profBlockedThreads -> c.waiting,
          profWorkThreads -> c.working)
    )

    if (asyncProfiler)
      ss += new AsyncProfilerSampler(sp, stackSamplers)
    // Most of the samplers
    samplerProviders.foreach { provider =>
      ss ++= provider.provide(sp)
    }

    ss += new Sampler[Seq[TopicCountSnap], Seq[TopicCountSnap]](
      sp,
      "topics",
      snapper = (_: Boolean) => topicAccumulators.values().asScala.map(_.snap).toSeq,
      process = (prevOpt: Option[Seq[TopicCountSnap]], curr: Seq[TopicCountSnap]) =>
        prevOpt.fold(curr)(curr.zip(_).map { case (c, p) =>
          c - p
        }),
      publish = { diff =>
        Elems(diff.map(_.elems): _*)
      }
    )

    ss += Diff(_ => StackAnalysis.numStacksPublished, numStacksPublished)
    ss += Diff(_ => StackAnalysis.numStacksNonDeduped, numStacksReferenced)

    ss += SnapNonZero(_ => BreadcrumbsKafkaPublisher.processedStats, kafkaMetrics)
    ss += SnapNonZero(_ => Breadcrumbs.getEnqueueFailures.stringMap, enqueueFailures)
    ss += DiffNonZero(_ => Breadcrumbs.getSourceCountAnalysis.stringMap, crumbCountAnalysis)
    ss += SnapNonZero(_ => Breadcrumbs.getSourceSnapAnalysis.stringMap, crumbSnapAnalysis)

    // This sampler must come last
    ss += new Sampler(
      sp,
      "duration",
      (_: Boolean) => System.currentTimeMillis() - _currentSnapTime,
      LATEST[Long],
      PUB(snapDuration))

    samplersLock.synchronized {
      samplers = samplers ++ ss.toList
    }
  }

  private val numCores = Runtime.getRuntime.availableProcessors()
  private def numThreads = schedulerCounters.flatMap(_.numThreads).sum
  private val staticElems: Elems =
    profStatsType -> Name ::
      cpuCores -> numCores ::
      engineRoot -> ChainedID.root.base ::
      Version.properties

  private val osBean: OperatingSystemMXBean =
    ManagementFactory.getOperatingSystemMXBean().asInstanceOf[OperatingSystemMXBean]

  private val samplingThread = new Thread {
    override def run(): Unit = {
      try {
        runSamplingThread()
      } catch {
        case NonFatal(t) =>
          warn("SamplingProfiler thread threw exception.  Shutting down.", t)
          try {
            shutdownNow()
          } catch {
            case NonFatal(t) =>
              warn("Exception shutting down samplers", t)
          }
      }
      info(s"Exiting $this")
    }
  }

  private def runSamplingThread(): Unit = {
    // Don't wait if we're shutting down, or we got a request to snap/publish
    // immediately.
    def shouldWait: Boolean = _stillPulsing && !_snapAndPublishImmediately

    do {
      snap()
      // Next snap at the next multiple of intervalMs since the epoch
      val nextSampleAt = ((_currentSnapTime + samplingPeriodMs / 2) / samplingPeriodMs + 1) * samplingPeriodMs
      // Actual publication occurs at some random time between now and then so all engines aren't publishing simultaneously
      val publishAt = _currentSnapTime +
        (Random.nextDouble() * 0.9 * (nextSampleAt - _currentSnapTime)).toLong
      waiter.synchronized {
        var t = System.currentTimeMillis()
        while (t < publishAt && shouldWait) {
          waiter.wait(publishAt - t)
          t = System.currentTimeMillis()
        }
      }
      publish()
      val doSnapNow = waiter.synchronized {
        var t = System.currentTimeMillis()
        while (t < nextSampleAt && shouldWait) {
          waiter.wait(nextSampleAt - t)
          t = System.currentTimeMillis()
        }
        // This flag will be set if we need to snap immediately for any reason, whether
        // because we're shutting down, or due to activity (e.g. a dist completion)
        // necessitating a snap now.
        _snapAndPublishImmediately
      }
      if (doSnapNow) {
        info("Triggering off-cycle snap and publish.")
        snap()
        publish()
        waiter.synchronized {
          _snapAndPublishImmediately = false
          waiter.notify()
        }
      }
    } while (_stillPulsing)
  }

  private var skippedPublications = 0
  private val publishedMetaKeyNames = ConcurrentHashMap.newKeySet[String]()
  private[sampling] def publish(): Unit = {

    val destinations = if (spoofRoot) publishAppInstances else Seq(defaultAppInstance)
    var isCanonical = true

    val pulseTimeData = snapTimeMs -> _currentSnapTime ::
      snapPeriod -> _actualPeriodMs ::
      snapTimeUTC -> Instant.ofEpochMilli(_currentSnapTime).toString ::
      snapEpochId -> (_currentSnapTime.toDouble / samplingPeriodMs + 0.5).toLong :: Elems.Nil;

    destinations.foreach { appInstance =>
      val id = appInstance.rootId

      val identificationData = appId -> appInstance.appId :: idealThreads -> numThreads :: staticElems

      // Each sampler returns its crumb source, plus a list of batches to publish.  We'll publish all the
      // 1st batch elems together, then all the 2nd, etc.
      val samplerResults: Seq[Map[Source, List[Elems]]] = samplers.map(_.elemss(id))

      if (pauseInactive && activityMeritingPublication.get() == 0) {
        hasPublished = true // Lie. No point publishing diagnostics if we meant to pause.
        skippedPublications += 1
        // Complain every power of 2
        if ((skippedPublications & (skippedPublications - 1)) == 0)
          Breadcrumbs.info(
            ownerId,
            PropertiesCrumb(
              _,
              ProfilerSource,
              profStatsType -> Name,
              logMsg -> s"Skipping publications due to inactivity: ${skippedPublications - skippedPublications / 2}.")
          )
        return
      }
      activityMeritingPublication.set(0)
      skippedPublications = 0

      // Group map by source.
      val batchesBySource = new util.HashMap[Source, Seq[List[Elems]]]()
      for {
        s2ess: Map[Source, List[Elems]] <- samplerResults
        (source, elemss) <- s2ess
      } batchesBySource.merge(source, Seq(elemss), _ ++ _)

      batchesBySource.asScala
        .foreach { case (source, b) =>
          var batchesForAllSamplers: Seq[List[Elems]] = b
          var iBatch = 0
          while (batchesForAllSamplers.nonEmpty) {
            iBatch += 1
            // Pull off the first batch for each sampler
            val (batch: Seq[Elems], rest) = batchesForAllSamplers.collect { case elems :: rest =>
              (elems, rest)
            }.unzip

            if (batch.nonEmpty) {
              val batchElems = nonEmptyElems(batch.toSeq: _*) // toSeq required by scala 2.13
              val pulseData: Elems = pulseTimeData ::: batchElems
              val newKeys: Map[String, Int] = batchElems.m.collect {
                case newKey if !publishedMetaKeyNames.contains(newKey.k.name) =>
                  publishedMetaKeyNames.add(newKey.k.name)
                  newKey.k.name -> newKey.k.meta.flags
              }.toMap
              val dataToPublish = isCanonical.thenSome(canonicalPub -> true) :: snapBatch -> iBatch ::
                pulse -> pulseData :: identificationData
              val toPublish: Elems =
                if (newKeys.nonEmpty) pulseMeta -> newKeys :: dataToPublish
                else
                  dataToPublish

              if (isCanonical)
                log.debug(s"Publishing batch=$iBatch to $source:${destinations.mkString(",")}: $toPublish")
              crumbConsumer.consume(id, source, toPublish)
            }
            batchesForAllSamplers = rest.filter(_.nonEmpty)
          }

        }
      isCanonical = false
    }
    val t = System.currentTimeMillis()
    crumbConsumer.flush()
    hasPublished = true
    log.info(s"Published to ${destinations.size} destinations")
  }

  private def snap(): Unit = {
    _currentSnapTime = System.currentTimeMillis()
    _actualPeriodMs = _currentSnapTime - _previousSnapTime
    val ss = samplersLock.synchronized(samplers)
    ss.foreach(_.snap())
    _nSnaps += 1 // 0 based
    _previousSnapTime = _currentSnapTime
    log.info(s"Snap #${_nSnaps}, ${ss.size} samplers")
  }

  def snapAndPublish(msSinceLastSnap: Long): Unit = {
    val t = System.currentTimeMillis()
    waiter.synchronized {
      val dt = t - _currentSnapTime
      if (dt > msSinceLastSnap && _stillPulsing && samplingThread.isAlive) {
        _snapAndPublishImmediately = true
        waiter.notifyAll()
        waiter.wait(cleanupWaitMs)
      }
    }
  }

  // By java convention, waitMs==0 means wait forever, but for us it means don't wait at all
  def shutdown(waitMs: Long): Unit = {
    waiter.synchronized {
      if (_stillPulsing && samplingThread.isAlive) {
        // Log the URL only during a normal shutdown
        logProfireUrl(ownerId.toString)
        warn("Shutting down sampling")
        enabled = false
        _snapAndPublishImmediately = waitMs > 0
        _stillPulsing = false
        waiter.notifyAll()
        if (waitMs > 0)
          waiter.wait(waitMs)
        samplers.foreach(_.shutdown())
      }
    }
  }

  sys.addShutdownHook { shutdown(cleanupWaitMs) }
  samplingThread.setName(Name + samplingThread.getId)
  samplingThread.setDaemon(true)
  samplingThread.start()
  def stillPulsing: Boolean = _stillPulsing && samplingThread.isAlive

}

trait SchedulerCounter {
  def getCounts: SamplingProfiler.SchedulerCounts
  def numThreads: Option[Int]
}

object SamplingProfiler {
  @volatile private[diagnostics] var configured = false

  val Name = "SamplingProfiler"
  val stackDataSource: Crumb.Source = SamplingProfilerSource
  val periodicSamplesSource: Crumb.Source = SamplingProfilerSource + ProfilerSource
  val SamplingIntervalSec = "optimus.sampling.sec"
  val DefaultSamplingIntervalSec = 60

  // minimum publication interval in the presence of "activity" (e.g. a distributed task completion)
  private val activityIntervalMs = PropertyUtils.get("optimus.sampling.activity.sec", 30) * 1000L

  val NANOSPERMILLI = 1000 * 1000L
  val MILLION = 1000 * 1000L
  val ALREADYMILLIS = 1L
  private val loadTime = System.currentTimeMillis() // approximately when ensureLoaded is called
  def nanosToMillis(ns: Long) = ns / NANOSPERMILLI

  def ensureLoadedIfEnabled(): Unit = {}
  def isEnabled = enabled
  private val auto = DiagnosticSettings.samplingProfilerStartsOn
  private var enabled = DiagnosticSettings.samplingProfilerStatic

  val configTimeoutSec = DiagnosticSettings.getLongProperty("optimus.sampling.config.timeout.sec", 60)

  private var unconfiguredTimerStarted = false

  private val asyncProfiler =
    enabled && AsyncProfilerSampler.numPrunedStacks > 0 &&
      AsyncProfilerIntegration.ensureLoadedIfEnabled() &&
      (!AsyncProfilerIntegration.isProfiling || {
        warn(s"async-profiler already running")
        false
      })

  private val numPulsers = new AtomicInteger(0)
  @volatile private var _instance: Option[SamplingProfiler] = None

  @volatile private var _initialAppId: String = "Optimus"
  def initialAppId = _initialAppId
  private[optimus] def applicationSetup(appId: String): Unit = {
    if (Objects.nonNull(appId))
      _initialAppId = appId
  }

  def stillPulsing(): Boolean = {
    _instance.exists(_.stillPulsing)
  }

  private def start(unconfiguredTimer: Boolean): Option[SamplingProfiler] = this.synchronized {
    if (enabled) {
      _instance orElse {
        val sp = new SamplingProfiler(ChainedID.root, DefaultSampleCrumbConsumer)
        if (unconfiguredTimer)
          startUnconfiguredTimer()
        _instance = Some(sp)
        _instance
      }
    } else None
  }

  if (auto) start(unconfiguredTimer = true)

  def start(): Unit = {
    start(unconfiguredTimer = false)
  }

  /*
   * If we require positive confirmation of configuration and haven't received it before the timeout,
   * shutdown.
   */
  private def startUnconfiguredTimer(): Unit = this.synchronized {
    if (
      DiagnosticSettings.samplingProfilerZkConfigurable && !DiagnosticSettings.samplingProfilerDefaultOn && !configured && !unconfiguredTimerStarted
    ) {
      unconfiguredTimerStarted = true;
      val timer = new Timer(true)
      timer.schedule(
        new TimerTask {
          override def run(): Unit = SamplingProfiler.synchronized {
            if (!configured) {
              val msg = s"Permanently disabling SP: Not configured within $configTimeoutSec seconds"
              SamplingProfiler.shutdownNow()
              configured = true
              warn(msg)
              Breadcrumbs.warn(
                ChainedID.root,
                PropertiesCrumb(
                  _,
                  ForensicSource,
                  Properties.logMsg -> msg :: Properties.severity -> "WARN" :: Version.properties)
              )
            }
          }
        },
        configTimeoutSec * 1000L
      )
    }
  }

  def instance(): Option[SamplingProfiler] = this.synchronized {
    if (enabled) {
      _instance orElse {
        if (auto) start(true) else None
      }
    } else None
  }

  def classToFrame(clz: Class[_], method: String): Option[String] =
    _instance.flatMap(_.stackSamplers.iterator.map(_.classToFrame(clz, method)).find(_.nonEmpty))
  def cleanClassName(clz: String): String =
    _instance.flatMap(_.stackSamplers.iterator.map(_.cleanClassName(clz)).find(_.nonEmpty)).getOrElse(clz)

  def shutdownNow(): Unit = SamplingProfiler.synchronized {
    instance().foreach(_.shutdown(-1))
    enabled = false
  }

  def running = enabled && _instance.exists(_.stillPulsing)

  // Ensure snap and publication if imminent shutdown is possible.
  def recordNowHint(): Unit = instance().foreach(_.snapAndPublish(activityIntervalMs))

  // Wake up publication if inactivity pause is in effect.
  def recordActivity(): Unit = instance().foreach(_.recordActivity())

  // Report the latest snapped value
  def LATEST[A]: (Option[A], A) => A = (_: Option[A], v: A) => v

  // Publish a single metric as a crumb property
  def PUB[V](key: Key[V]): (V, ChainedID) => Elems = (v: V, _) => Elems(key -> v)

  private val GlobalMaxWarnings = new AtomicInteger(100)

  def warn(msg: String, t: Throwable = null, counter: AtomicInteger = GlobalMaxWarnings): Unit =
    instance().foreach(_.warn(msg, t, counter))

  // for testing
  def NOPUB: Any => Elems = (v: Any) => Elems.Nil

  final class Util(sp: SamplingProfiler) {
    object Snap {
      // Create a simple scalar sampler that publishes to a single key.
      def apply[N](snapper: Boolean => N, key: Key[N])(implicit loc: SourceLocation): Sampler[N, N] =
        new Sampler(sp, loc.toString, snapper, LATEST[N], PUB(key))

      // Create an almost-simple-sampler with just one callback that produces Elems ready
      // for publication.
      def apply(snapper: Boolean => Elems)(implicit loc: SourceLocation) =
        new Sampler[Elems, Elems](sp, loc.toString, snapper, LATEST[Elems], (elems, _) => elems)
    }

    trait Minus[N] {
      def minus(a: N, b: N): N
      def isNonZero(a: N): Boolean
      def nonZero(a: N): Option[N]
    }

    implicit def numericMinus[N: Numeric]: Minus[N] = new Minus[N] {
      val num = implicitly[Numeric[N]]
      def minus(a: N, b: N) = num.minus(a, b)
      def isNonZero(v: N): Boolean = num.zero != v
      def nonZero(v: N): Option[N] = if (num.zero == v) None else Some(v)
    }

    implicit def mapMinus[K, V: Minus]: Minus[Map[K, V]] = new Minus[Map[K, V]] {
      val num = implicitly[Minus[V]]
      def minus(a: Map[K, V], b: Map[K, V]): Map[K, V] = {
        val keys = a.keySet
        keys.map { k =>
          k -> b.get(k).fold(a(k))(bv => num.minus(a(k), bv))
        }.toMap
      }
      override def isNonZero(a: Map[K, V]): Boolean = a.exists(kv => num.isNonZero(kv._2))
      def nonZero(m: Map[K, V]): Option[Map[K, V]] = {
        val filtered = m.filter(kv => num.isNonZero(kv._2))
        if (filtered.nonEmpty) Some(filtered) else None
      }
    }

    // Report the numeric difference between the latest and previous value
    def MINUS[N](implicit num: Minus[N]) = (prev: Option[N], v: N) => prev.fold(v)(p => num.minus(v, p))

    final implicit class FilterValues[K, V](val map: Map[K, V]) {
      def stringMap: Map[String, V] = map.map { case (k, v) =>
        k.toString -> v
      }
    }

    // Simple snap, publishing difference from previous numeric value to a single property
    def Diff[N](snapper: Boolean => N, key: Key[N])(implicit num: Minus[N], loc: SourceLocation): Sampler[N, N] = {
      new Sampler(sp, loc.toString, snapper, MINUS(num), PUB(key))
    }

    // Like Diff, but never publishes zero diffs
    def DiffNonZero[N](snapper: Boolean => N, key: Key[N])(implicit
        num: Minus[N],
        loc: SourceLocation): Sampler[N, N] = {
      new Sampler(sp, loc.toString, snapper, MINUS(num), (v, _) => Elems(key.maybe(num.nonZero(v))))
    }

    def SnapNonZero[N](snapper: Boolean => N, key: Key[N])(implicit
        num: Minus[N],
        loc: SourceLocation): Sampler[N, N] = {
      new Sampler(sp, loc.toString, snapper, LATEST[N], (v, _) => Elems(key.maybe(num.nonZero(v))))
    }

    def SnapElems(name: String)(snapper: Elems): Unit = {
      new Sampler[Elems, Elems](sp, name, _ => snapper, LATEST[Elems], (v, _) => v)
    }

  }

  /**
   * @param snapper
   *   (isFirstStamp: Boolean) => T : snap a new value
   * @param process
   *   (Option[T], T) => V : combine new snap with previous to get value to publish
   * @param publish
   *   V => Elems : produce list of crumb elements to be published
   * @tparam T
   * @tparam V
   */

  private[sampling] final class Sampler[T, V](
      owner: SamplingProfiler,
      name: String,
      snapper: Boolean => T,
      process: (Option[T], T) => V,
      publish: (V, ChainedID) => Elems,
  ) extends SamplerTrait[T, V] {

    override def toString: String = s"${super.toString}($name)"

    override val sp: SamplingProfiler = owner
    override protected def snap(firstTime: Boolean): T = snapper(firstTime)
    override protected def transform(prev: Option[T], curr: T): V = process(prev, curr)
    override protected def elemss(result: V, id: ChainedID): Map[Source, List[Elems]] =
      Map(periodicSamplesSource -> (publish(result, id) :: Nil))

    def this(
        owner: SamplingProfiler,
        name: String,
        snapper: Boolean => T,
        process: (Option[T], T) => V,
        publish: V => Elems) =
      this(owner, name, snapper, process, (v: V, _: ChainedID) => publish(v))

    override def shutdown(): Unit = {}
  }

  trait SamplerTrait[T, V] {
    protected val sp: SamplingProfiler
    protected def snap(firstTime: Boolean): T
    protected def transform(prev: Option[T], curr: T): V
    protected def elemss(result: V, id: ChainedID): Map[Source, List[Elems]]

    def shutdown(): Unit

    private var lastSnapTime: Long = -1
    private var lastProcessTime: Long = -1
    private var currSample: Option[T] = None
    private var prevSample: Option[T] = None
    private var currValue: Option[V] = None

    import sp._currentSnapTime

    private var enabled = true

    private def disable(where: String, t: Throwable): Unit = if (enabled) {
      sp.warn(s"Disabling sampler $this due to error $where; other samplers should be unaffected.", t)
      BaseSamplers.increment(Properties.numSamplersDisabled, 1)
      enabled = false
      try {
        shutdown()
      } catch {
        case NonFatal(t) =>
          sp.warn(s"Error shutting down sampler $this, but it remains disabled.", t)
      }
    }

    private def safe[T](where: String, default: => T)(f: => T): T = if (enabled) {
      try (f)
      catch {
        case t: Throwable => // Want to catch errors like NoSuchMethod, so use Throwable here
          disable(where, t)
          default
      }
    } else default

    final def snap(): Unit = safe("snap", ()) {
      // Only snap once per epoch
      if (lastSnapTime < _currentSnapTime || currSample.isEmpty) {
        val t = snap(currSample.isEmpty)
        prevSample = currSample
        currSample = Some(t)
        lastSnapTime = _currentSnapTime
      }
    }

    final private def processedValue(): Option[V] = {
      currValue match {
        // Only process once per epoch
        case ret @ Some(v) if lastProcessTime >= _currentSnapTime =>
          ret
        case _ =>
          snap()
          safe[Option[V]]("processing", None) {
            currSample.map { t =>
              val v = transform(prevSample, t)
              lastProcessTime = _currentSnapTime
              currValue = Some(v)
              v
            }
          }
      }
    }

    final def elemss(id: ChainedID): Map[Source, List[Elems]] =
      safe[Map[Source, List[Elems]]]("elem generating", Map.empty) {
        processedValue() match {
          case Some(v) => elemss(v, id)
          case None    => Map.empty
        }
      }
  }

  final case class SchedulerCounts(working: Int, waiting: Int, blocked: Int) {
    def +(sc: SchedulerCounts) = copy(
      working = working + sc.working,
      waiting = waiting + sc.waiting,
      blocked = blocked + sc.blocked
    )
  }
  object SchedulerCounts {
    def empty = SchedulerCounts(0, 0, 0)
  }

  final case class LoadData(cpuLoad: Double, graphUtilization: Double) {
    def round(n: Int) = LoadData((cpuLoad * n).round.toDouble / n, (graphUtilization * n).round.toDouble / n)
  }

  // Generic facility for accumulating and publishing data like
  //  Properties.pluginTimes -> {Adapted -> { DAL -> 36, Pluggy -> 100}, Inflight -> { DAL -> ...
  private type TopicCountMap = Map[String, Map[String, Long]]

  private[SamplingProfiler] final case class TopicCountSnap private[SamplingProfiler] (
      property: Key[TopicCountMap],
      map: TopicCountMap) {
    def elems: Elems = if (map.size > 0) Elems(property -> map) else Elems.Nil
    def -(other: TopicCountSnap): TopicCountSnap = {
      assert(property == other.property, "Internal sampling profiler error")
      TopicCountSnap(property, diff(map, other.map))
    }
  }

  final class TopicAccumulator private[SamplingProfiler] (property: Key[TopicCountMap]) {
    private val topics = new ConcurrentHashMap[String, ConcurrentHashMap[String, Long]]()
    def accumulate(topic: String, key: String, value: Long): Unit = {
      val map = topics.computeIfAbsent(topic, _ => new ConcurrentHashMap[String, Long]())
      map.merge(key, value, _ + _)
    }
    private[SamplingProfiler] def snap: TopicCountSnap = {
      val m = topics.asScala.map { case (topic, map) =>
        topic -> map.asScala.toMap
      }.toMap
      TopicCountSnap(property, m)
    }
  }

  private val topicAccumulators = new ConcurrentHashMap[Key[TopicCountMap], TopicAccumulator]()

  // Get/register a new topic accumulator
  def topicAccumulator(key: Key[TopicCountMap]): TopicAccumulator =
    topicAccumulators.computeIfAbsent(key, _ => new TopicAccumulator(key))

  private def diff(
      a: Map[String, Map[String, Long]],
      b: Map[String, Map[String, Long]]): Map[String, Map[String, Long]] = {
    a.map { case (topic, ma) =>
      b.get(topic) match {
        case None => topic -> ma.filter(_._2 > 0L)
        case Some(mb) =>
          topic -> {
            ma.map { case (key, va) =>
              key -> (va - mb.getOrElse(key, 0L))
            }.filter(_._2 > 0L)
          }
      }
    }.filter(_._2.size > 0)
  }

}
