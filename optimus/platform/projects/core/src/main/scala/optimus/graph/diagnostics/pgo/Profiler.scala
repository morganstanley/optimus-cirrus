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
package optimus.graph.diagnostics.pgo

import msjava.slf4jutils.scalalog.Logger
import optimus.config.CacheConfig
import optimus.config.NodeCacheConfigs
import optimus.graph._
import optimus.graph.diagnostics.GivenBlockProfile
import optimus.graph.diagnostics.NodeName
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.PScenarioStack
import optimus.graph.diagnostics.ProfilerUIControl
import optimus.graph.diagnostics.gridprofiler.GridProfiler
import optimus.graph.diagnostics.gridprofiler.GridProfiler.ProfilerOutputStrings
import optimus.graph.diagnostics.pgo.ConfigWriter.withWriter
import optimus.graph.loom.LNodeClsID
import optimus.platform.EvaluationContext
import optimus.platform.ScenarioStack
import optimus.platform.stats.Utils

import java.io._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

//dont use scala StringBuilder
import java.lang.{StringBuilder => JStringBuilder}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object Profiler {
  ////////////////////////////////////////////////////////////////////////////////////////
  // Debug methods - output log messages                                                //
  ////////////////////////////////////////////////////////////////////////////////////////
  val u: Utils.type = Utils
  val log: Logger = msjava.slf4jutils.scalalog.getLogger("PROFILER")
  def lnlog(str: String): Unit = println(str)
  def lnlog(): Unit = lnlog("")
  def lnlog(w: Int): Unit = lnlog(" " * w)
  def padTo(b: Boolean, w: Int): String = b.toString.padTo(w, ' ')
  def padTo(str: String, w: Int): String = str.padTo(w, ' ')

  val maxCnt = new AtomicInteger
  final def showSettings(show: Boolean = true, sysProps: Boolean = false, showEnvironment: Boolean = false): Unit = {
    if (show) {
      val pad = 14
      val fullW = 44
      lnlog(fullW)
      lnlog("        ************ Entered PROFILER ****************")
      lnlog("                 Init Millis: " + System.currentTimeMillis.toString.padTo(pad, ' '))
      lnlog(
        "                      onGrid: " + padTo(DiagnosticSettings.onGrid, pad)
      ) // TODO (OPTIMUS-0000): see onGrid above
      lnlog("             profilingRemote: " + padTo(DiagnosticSettings.profileRemote, pad))
      lnlog("                 profileName: " + padTo(OGTrace.getTraceMode.name, pad))
      lnlog(fullW)

      if (sysProps) {
        lnlog()
        val lst =
          u.sysPropertiesFormat(List(u.sysPropClassPath, u.sysPropLibPath), rightAdjust = true, splitAllPaths = true)
        val max = lst.maxBy[Int](s => s.length)
        lnlog(u.strCenter(" System Properties ", max.length, '*'))
        lst.foreach(lnlog)
      }
      if (showEnvironment) {
        val lst = u.envFormat(u.envKeysSorted)
        lst.foreach(lnlog)
      }
    }
  }

  showSettings(show = false)
  // Patch out showing params

  var onsstack_trace: ( /*ssu*/ ScenarioStack, /*hit:*/ Boolean) => Unit = _

  val locationCache = new ConcurrentHashMap[AnyRef, StackTraceElement]
  val sstack_roots = new ConcurrentHashMap[AnyRef, GivenBlockProfile]
  val sstack_id = new AtomicInteger(0)

  private def cleanupLocMarker(locMarker: AnyRef): AnyRef = locMarker match {
    case clsID: LNodeClsID if clsID.isDynamic => clsID._clsID().asInstanceOf[AnyRef]
    case null                                 => null
    case _                                    => locMarker.getClass
  }

  private def resolveLocation(locMarker: AnyRef): StackTraceElement = locMarker match {
    case clsID: LNodeClsID => clsID.stackTraceElem()
    case _ =>
      val cleanLocMarker = cleanupLocMarker(locMarker)
      val loc = if (cleanLocMarker eq null) null else locationCache.get(cleanLocMarker)
      if (loc ne null) loc
      else {
        val stack = Thread.currentThread().getStackTrace
        var i = 1
        var location = stack(1)
        while (i < stack.length) {
          location = stack(i)
          val clsName = location.getClassName
          if (
            (clsName.startsWith("optimus.graph")
              || clsName.startsWith("optimus.platform")
              || clsName.startsWith("optimus.core")
              || clsName.startsWith("optimus.config")
              || clsName.startsWith("scala")
              || clsName.startsWith("java")) &&
            !clsName.startsWith("optimus.platform.tests")
          )
            i += 1
          else
            i = Int.MaxValue
        }
        if (cleanLocMarker ne null) locationCache.putIfAbsent(cleanLocMarker, location)
        location // Our best guess for the location that created 'ScenarioStack'
      }
  }

  final def saveProfile(fileName: String): Unit = writeProfileData(new FileWriter(fileName))
  final def writeProfileData(out: Writer): Unit = writeProfileData(out, getProfileData())

  final def writeProfileData(out: Writer, tags: Iterable[PNodeTaskInfo]): Unit = {
    EvaluationContext.verifyOffGraph(ignoreNonCaching = false)

    out.write(
      "started, evicted, age, pia_h, pia_m, pia_time, time, child_time, self_time, wall_self_time, cacheMem, gcNativeMem, flags, recommend, pname, clsname, rtype, engine_start_timestamp, engine_end_timestamp, engine_time, roundtrip_time, engine_ID, dist_job_name, comment \n")
    out.close()
  }

  // Public API/main entry point for writing optconf file
  def autoGenerateConfig(
      confName: String,
      allProfileData: Seq[PNodeTaskInfo],
      settings: ConfigWriterSettings = ConfigWriterSettings.default,
      isPgoGen: Boolean = false): String = {
    val (combinedProfileData, cacheConfigs) = combineData(allProfileData)
    writeConfigWithCaches(confName, combinedProfileData, cacheConfigs, settings, isPgoGen = isPgoGen)
  }

  def pgoCacheDisabledPntis(pntis: Seq[PNodeTaskInfo]): (Seq[PNodeTaskInfo], Seq[PNodeTaskInfo]) =
    pntis.partition(pgoWouldDisableCache)

  def pgoWouldDisableCache(pnti: PNodeTaskInfo): Boolean =
    DisableCache.autoConfigure(pnti, ConfigWriterSettings.default.thresholds)

  /** For testing only, note the cacheConfigs = NodeCacheConfigs.getCacheConfigs.values */
  def autoGenerateConfig(out: Writer, allProfileData: Seq[PNodeTaskInfo], settings: ConfigWriterSettings): Unit = {
    val combinedProfileData = combineTraces(allProfileData)
    val cacheConfigs = NodeCacheConfigs.getCacheConfigs.values
    try {
      val configWriter = new ConfigWriter(out, combinedProfileData, cacheConfigs, settings)
      configWriter.write(isPgoGen = false)
    } finally out.close()
  }

  def autoGenerateConfig(
      out: Writer,
      allProfileData: Seq[PNodeTaskInfo],
      cacheConfigs: Iterable[CacheConfig],
      settings: ConfigWriterSettings): Unit = {
    val combinedProfileData = combineTraces(allProfileData)
    try {
      val configWriter = new ConfigWriter(out, combinedProfileData, cacheConfigs, settings)
      configWriter.write(isPgoGen = false)
    } finally out.close()
  }

  private def writeConfigWithCaches(
      confName: String,
      combinedProfileData: Seq[PNodeTaskInfo],
      cacheConfigs: Iterable[CacheConfig],
      settings: ConfigWriterSettings,
      isPgoGen: Boolean): String = {
    val configName = optconfFilename(confName)
    val summary = withWriter(configName) { out =>
      val configWriter = new ConfigWriter(out, combinedProfileData, cacheConfigs, settings)
      configWriter.write(isPgoGen = isPgoGen)
      configWriter.logSummary()
    }
    val path = configName.replace("\\", "/")
    val modes = settings.modes
    log.info(s"New optimus config based on modes ${modes.mkString(",")} written to file:///$path")
    summary
  }

  /** Helper used in RegressionsConfigApps */
  private[optimus] def autoGenerateConfigWithCacheNames(
      confName: String,
      allProfileData: Seq[PNodeTaskInfo],
      cacheNames: Set[String],
      settings: ConfigWriterSettings,
      isPgoGen: Boolean): Unit = {
    val combinedProfileData = combineTraces(allProfileData)
    val sharedCaches = getConfigs(cacheNames)
    writeConfigWithCaches(confName, combinedProfileData, sharedCaches, settings, isPgoGen)
  }

  /** Helper used in RegressionsConfigApps to write output to string */
  private[optimus] def autoGenerateConfigWithCacheNames(
      allProfileData: Seq[PNodeTaskInfo],
      cacheNames: Set[String],
      settings: ConfigWriterSettings,
      isPgoGen: Boolean): String = {
    val combinedProfileData = combineTraces(allProfileData)
    val sharedCaches = getConfigs(cacheNames)
    val out = new StringWriter()
    try {
      val configWriter = new ConfigWriter(out, combinedProfileData, sharedCaches, settings)
      configWriter.write(isPgoGen = isPgoGen)
    } finally out.close()
    out.toString
  }

  private[optimus] def optconfFilename(initialFileName: String): String = {
    val extension = ProfilerOutputStrings.optconfExtension
    val fullExt = if (initialFileName.endsWith(".")) extension else s".$extension"
    if (initialFileName.endsWith(s".$extension")) initialFileName else s"$initialFileName$fullExt"
  }

  // combine all PNTIs that have the same fullName
  private[optimus] def combineTraces(pntis: Seq[PNodeTaskInfo]): Seq[PNodeTaskInfo] =
    combinePNTIs(pntis).values.toSeq

  // combine all PNTIs that have the same fullName, enriched by enqueuer name.
  private[optimus] def combinePNTIs(pntis: Seq[PNodeTaskInfo]): Map[String, PNodeTaskInfo] = {
    val byName = pntis.groupBy(_.fullName)
    val eid2name: Map[Int, String] = for {
      (k, pntis) <- byName
      ep <- pntis.find(_.enqueuingProperty < 0)
    } yield (-ep.enqueuingProperty, k)
    byName.transform { (_, v) =>
      val red = v.reduce(_.combine(_))
      eid2name.get(Math.abs(red.enqueuingProperty)).foreach(red.enqueuingPropertyName = _)
      red
    }
  }

  // The PNTIs in ogtrace may have been configured with custom caches. We need to preserve those for output - from optconf
  // Note: this means that externally configured caches will be registered as shared caches, even if the cache is not sharable [SEE_EXTERNAL_SHARABLE_CACHE]
  private[optimus] def getCaches(pntis: Seq[PNodeTaskInfo]): Set[String] =
    pntis
      .filter(pn =>
        (pn.cacheName ne null) && (pn.externallyConfiguredCache || pn.profilerUIConfigured)) // [SEE_EXT_CONFIGURED]
      .map(_.cacheName)
      .toSet

  private[diagnostics] def getConfigs(cacheNames: Set[String]): Iterable[CacheConfig] = {
    // Global and externally-configured shared caches should also be added even if they don't appear in any PNTI
    val globalAndSharedCaches = NodeCacheConfigs.getCacheConfigs.values.filter(_.name.nonEmpty)
    // we take allCaches, and for any cache in PNTI-originated cache names that is NOT in allCaches, we make up a sharable cache to represent it in the config
    globalAndSharedCaches ++ cacheNames
      .filter(n => !globalAndSharedCaches.exists(_.name == n))
      .map(n => CacheConfig(n))
  }

  private[diagnostics] def combineData(
      allProfileData: Seq[PNodeTaskInfo]): (Seq[PNodeTaskInfo], Iterable[CacheConfig]) = {
    val combinedProfileData = combineTraces(allProfileData)
    val cacheNames = getCaches(combinedProfileData)
    (combinedProfileData, getConfigs(cacheNames))
  }

  final def getLocalHotspots(blk: Int = OGTrace.BLOCK_ID_ALL): Map[String, PNodeTaskInfo] =
    Profiler.combinePNTIs(OGTrace.liveReader.getScopedTaskInfos(blk).asScala)

  final def getProfileData(
      alwaysIncludeTweaked: Boolean = false,
      includeTweakableNotTweaked: Boolean = false): ArrayBuffer[PNodeTaskInfo] = {
    val pntis =
      OGTrace.liveReader.getScopedTaskInfos(OGTrace.BLOCK_ID_ALL, alwaysIncludeTweaked, includeTweakableNotTweaked)
    ArrayBuffer(pntis.asScala: _*)
  }

  final def resetAll(): Unit = {
    OGTrace.reset()
    GridProfiler.resetAll()
    try {
      if (GCNative.loaded)
        GCNative.resetForNewTask()
    } catch {
      case _: UnsatisfiedLinkError => log.debug("gcnative is not loaded, ignoring reset for new task")
    }
  }

  final def dumpSStackUsage(full: Boolean, showNCTweaks: Boolean): Unit = {
    val sb = new JStringBuilder
    sb.append(" id  miss     hit  selfTime (ms)    location\n")
    def walk(seq: Iterable[GivenBlockProfile]): Unit = {
      for (us <- seq) {
        sb.append(f"${us.id}%3d ${us.level}%3d ${us.getMisses}%8d ${us.getHits}%8d ${us.getSelfTimeScaled}%11.1f")
        sb.append("   ")
        sb.append(us.formattedLocation(full))
        sb.append('\n')
        if (showNCTweaks)
          for (nttwk <- us.getByNameTweaks.keySet().asScala) {
            sb.append("                                -> ")
            sb.append(NodeName.nameAndSource(nttwk))
            sb.append('\n')
          }

        if (!us.children.isEmpty)
          walk(us.children.values().asScala)
      }
    }
    walk(sstack_roots.values().asScala)

    val output = sb.toString
    ProfilerUIControl.outputOnConsole(output)
  }

  final def t_sstack_root(ss: ScenarioStack, locMarker: AnyRef = null): GivenBlockProfile = {
    val ssu = new GivenBlockProfile(resolveLocation(locMarker), 0)
    val ossu = sstack_roots.putIfAbsent(ssu.location, ssu)
    val rssu = if (ossu eq null) ssu else ossu
    ss.pss = new PScenarioStack(givenProfile = rssu)
    rssu
  }

  final def t_sstack_usage(ss: ScenarioStack, locMarker: AnyRef, hit: Boolean): Unit = {
    val ssu =
      if (ss.givenBlockProfile ne null) ss.givenBlockProfile
      else {
        if ((ss.parent eq null) || (ss.parent.givenBlockProfile eq null)) t_sstack_root(ss, locMarker)
        else {
          val pusage = ss.parent.givenBlockProfile
          val counts = pusage.children.computeIfAbsent(
            cleanupLocMarker(locMarker),
            _ => new GivenBlockProfile(resolveLocation(locMarker), pusage.level + 1)
          )
          ss.pss = new PScenarioStack(givenProfile = counts)
          counts
        }
      }

    if (ss._cacheID ne null) {
      for (twk <- ss.expandedTweaks) {
        val cgen = twk.tweakTemplate.computeGenerator
        if ((cgen ne null) && !cgen.isInstanceOf[AlreadyCompletedNode[_]])
          ssu.addByNameTweak(cgen.getClass, "")
      }
    }

    if (hit) ssu.incHits()
    else ssu.incMisses()

    // This callback ability is here to be able to add debug tracers in the callbacks
    if (onsstack_trace ne null)
      try { onsstack_trace(ss, hit) }
      catch {
        case ex: Throwable => ProfilerUIControl.outputOnConsole(ex.toString)
      }
  }
}

class TraceTag2(var id: Int, val pname: String, val ename: String) extends Serializable {
  var flags = 0L
  var start = 0
  var calls = 0
  var totalTime = 0L
  var selfTime = 0L
  def totalChildTime: Long = totalTime - selfTime

  var getIfPresentTime = 0L
  var getIfPresentHit = 0
  var getIfPresentMiss = 0
  var putIfAbsentTime = 0L
  var putIfAbsentHit = 0
  var putIfAbsentMiss = 0
  var evicted = 0
  var invalidated = 0
  var age = 0
  var returnType: String = _
  var cacheMemoryEffect: Long = 0
  var gcNativeEffect: Long = 0

  var engineSentTimestamp = 0L
  var engineReturnedTimestamp = 0L
  var engineStartTimestamp = 0L
  var engineFinishTimestamp = 0L
  var distributionTime = 0L
  // nanoseconds
  var engineID: String = _
  var distJobName: String = _
  var comment: String = ""
  var engineReturnTime = 0L // NOT USED - just to match w new Profiler

  def cacheHit: Int = putIfAbsentHit + getIfPresentHit
  def cacheMiss: Int = putIfAbsentMiss + getIfPresentMiss
  def cacheTime: Long = getIfPresentTime + putIfAbsentTime
  def fullName: String = ename + "." + pname
  def avgSelfTime: Long = if (start == 0) 0L else selfTime / start

  @transient
  val callsTo = new ConcurrentHashMap[NodeTaskInfo, AnyRef]()

  def recommend: String =
    (if (shouldNotCache) "-$" else "") + (if ((NodeTaskInfo.ASYNC_NEEDED & flags) == 0) "-" else "")

  def shouldNotCache: Boolean = avgSelfTime * cacheHit < cacheTime
  def hasNativeMarker: Boolean = (flags & NodeTaskInfo.HOLDS_NATIVE) != 0

  def combine(t: TraceTag2): TraceTag2 = {
    val tag = new TraceTag2(id, pname, ename)
    tag.returnType = if (returnType ne null) returnType else t.returnType
    tag.flags = flags | t.flags
    tag.start = start + t.start
    tag.calls = calls + t.calls
    tag.totalTime = totalTime + t.totalTime
    tag.selfTime = selfTime + t.selfTime
    tag.getIfPresentTime = getIfPresentTime + t.getIfPresentTime
    tag.getIfPresentHit = getIfPresentHit + t.getIfPresentHit
    tag.getIfPresentMiss = getIfPresentMiss + t.getIfPresentMiss
    tag.putIfAbsentTime = putIfAbsentTime + t.putIfAbsentTime
    tag.putIfAbsentHit = putIfAbsentHit + t.putIfAbsentHit
    tag.putIfAbsentMiss = putIfAbsentMiss + t.putIfAbsentMiss
    tag.evicted = evicted + t.evicted
    tag.invalidated = invalidated + t.invalidated
    tag.age = Math.max(age, tag.age)
    tag.cacheMemoryEffect = cacheMemoryEffect + t.cacheMemoryEffect
    tag.gcNativeEffect = gcNativeEffect + t.gcNativeEffect
    tag
  }

  override def toString: String = {
    NodeTaskInfo.flagsAsString(flags) + "\n" +
      "start: " + start + "\n" +
      "totalTime: " + (totalTime * 1e-6) + "\n" +
      "selfTime: " + (selfTime * 1e-6) + "\n" +
      "totalChildTime: " + (totalChildTime * 1e-6) + "\n" +
      "putIfAbsentTime: " + (putIfAbsentTime * 1e-6) + "\n" +
      "putIfAbsentHit: " + putIfAbsentHit + "\n" +
      "putIfAbsentMiss: " + putIfAbsentMiss + "\n"
  }

  override def hashCode(): Int =
    (if (ename ne null) ename.hashCode() * 41 else 0) + (if (pname ne null) pname.hashCode() else 0)
  override def equals(other: Any): Boolean = other match {
    case t: TraceTag2 => t.ename == ename && t.pname == pname
    case _            => false
  }

}
