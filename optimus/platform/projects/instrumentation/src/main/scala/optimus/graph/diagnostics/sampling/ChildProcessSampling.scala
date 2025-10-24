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
import optimus.graph.AsyncProfilerIntegration
import optimus.utils.PropertyUtils
import optimus.breadcrumbs.crumbs.Properties._
import optimus.graph.AwaitStackManagement
import optimus.graph.Awaitable
import optimus.graph.Launchable
import optimus.graph.diagnostics.ap.StackAnalysis
import optimus.logging.Pid
import optimus.platform.util.Log
import optimus.utils.FileUtils
import optimus.utils.MiscUtils.Endoish._
import optimus.utils.UnixLibUtils

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object ChildProcessSampling extends Log {
  import AsyncProfilerSampler._

  private val childProcessIndex = new AtomicInteger(0)
  private val childId2Tag = new ConcurrentHashMap[Int, String]()
  // Will be appended to temporary file being written by async-profiler in child
  // processes.
  private val atomicSuffix = "_TEMP_";

  lazy private val childStackDir: Path = {
    FileUtils.deleteOnExit(Files.createTempDirectory(Paths.get("/tmp/"), "ap-stacks"))
  }

  private val childStaxPrefix = s"stax-${Pid.pidOrZero()}"

  /**
   * Assign an ID to identify this child process.  The caller is responsible for passing this to getChildEnv.
   */
  def newChildId(): Int = if (allowChildProfiling && SamplingProfiler.stillPulsing()) {
    if (childProcessIndex.get() == 0)
      Files.createDirectories(childStackDir)
    val i = childProcessIndex.incrementAndGet()
    val tag = s"$childStaxPrefix-$i"
    childId2Tag.put(i, tag)
    AsyncProfilerIntegration.externalContext(-1, tag)
    i
  } else 0
  def removeChildId(id: Int): Unit = childId2Tag.remove(id)

  // Child process preload path will contain the libasyncProfiler and libjemalloc paths found in the parent process.
  lazy private val (defaultPreloads: Seq[String], nativeMem: Boolean) = {
    val (pre, nm) = UnixLibUtils.findLibrary("libasyncProfiler.so") match {
      case None =>
        log.warn("libasyncProfiler.so not found in /proc/self/maps")
        (Seq.empty, false)
      case Some(ap) =>
        UnixLibUtils.findLibrary("libjemalloc.so") match {
          case Some(je) => (Seq(ap, je), true)
          case None     => (Seq(ap), false)
        }
    }
    (pre, nm)
  }

  private lazy val childLoopPeriodSec =
    Math.max(SamplingProfiler.instance().fold(0)(_.periodSec) / childProfilingFrequency, 1)
  private lazy val childAsyncProfilerCommand = PropertyUtils.get(
    "async.profiler.child.command",
    s"start," +
      s"event=${AsyncProfilerIntegration.containerSafeEvent("cpu")}," +
      // alloc value here is irrelevant, other than triggering allocation sampling
      s"alloc=1048575b,live,collapsed,memoframes,total," +
      // dump new collapsed stack output at specified periodicity
      s"loop=${childLoopPeriodSec}s," +
      s"cstack=$cstack," +
      // same stack sampling interval as parent
      s"interval=${AsyncProfilerSampler.apInterval},etypeframes,persist,jemalloc," +
      // suffix to append to temporary files before being atomically moved to permanent name
      s"atomicfile=$atomicSuffix," +
      s"loglevel=${AsyncProfilerSampler.apLogLevel}"
  )

  /**
   * Construct environment variables to be set in the child process
   */
  def getChildEnv(id: Int, morePreloads: Seq[String] = Seq.empty): Map[String, String] = if (
    !allowChildProfiling || !SamplingProfiler.stillPulsing() || defaultPreloads.isEmpty || !childId2Tag.containsKey(id)
  )
    Map.empty
  else {
    val staxId = childId2Tag.get(id)
    val preLoads = (morePreloads.filter(f => Files.isExecutable(Paths.get(f))) ++ defaultPreloads).mkString(":")
    val env = Map(
      // Preload async-profiler and possibly jemalloc
      "LD_PRELOAD" -> preLoads,
      // Async-profiler will extract profiling arguments from this environment variable.
      // shmcontext sets the shared memory path for communicating context, which in
      // our case is the rolling hash of the await stacks.
      // Rolling output file will always have our prefix
      "ASPROF_COMMAND" -> s"$childAsyncProfilerCommand,shmcontext=${staxId},file=${childStackDir}/${staxId}-%p-%t"
    ).applyIf(nativeMem)(x =>
      // If profiling native memory, set MALLOC_CONF to enable jemalloc profiling
      x + (s"MALLOC_CONF" -> s"prof:true,prof_prefix:${childStackDir}/jeprof,prof_active:false,lg_prof_sample:20"))
    log.info(s"Child profiler env: $env")
    env
  }

  def getNewChildEnv(morePreloads: Seq[String] = Seq.empty): (Int, Map[String, String]) = {
    val id = newChildId()
    (id, getChildEnv(id, morePreloads))
  }

  def reapChildStacks(): Iterable[TimedStack] = if (childId2Tag.size() == 0) Iterable.empty
  else
    try {
      val linesIterator: Iterator[String] = Files
        .list(childStackDir)
        .iterator()
        .asScala
        .flatMap { path: Path =>
          val name = path.getFileName.toString
          if (name.startsWith(childStaxPrefix) && !name.contains(atomicSuffix)) {
            val content = Files.readAllLines(path).asScala
            Files.delete(path)
            content
          } else Nil
        }
      // Convert to Buffer explicitly, since toIterable would default to Stream
      val stacks = linesIterator.map(StackAnalysis.stackLineToTimedStack).toBuffer
      log.info(s"Reaped ${stacks.size} child stacks from $childStackDir")
      stacks
    } catch {
      case NonFatal(e) =>
        log.warn("Error reaping child stacks", e)
        Iterable.empty
    }

  // Set context via shared memory in child processes
  def setChildContext(id: Int, ctx: Long): Unit = if (
    allowChildProfiling && AsyncProfilerIntegration.ensureLoadedIfEnabled()
  ) {
    Option(childId2Tag.get(id)).foreach(shmpath => AsyncProfilerIntegration.externalContext(ctx, shmpath))
  }

  def setChildContext(childId: Int, launchable: Launchable): Unit = if (
    allowChildProfiling && AsyncProfilerIntegration.ensureLoadedIfEnabled() && launchable.getLauncherStackHash != 0
  ) {
    // Ensure that the stack is cached by id
    AwaitStackManagement.awaitStack(launchable)
    setChildContext(childId, launchable.stackId)
  }

}
