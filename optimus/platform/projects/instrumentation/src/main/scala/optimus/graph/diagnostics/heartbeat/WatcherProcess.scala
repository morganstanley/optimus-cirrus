package optimus.graph.diagnostics.heartbeat
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

import msjava.zkapi.ZkaAttr
import msjava.zkapi.ZkaConfig
import msjava.zkapi.internal.ZkaContext
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Properties
import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.OSInformation
import optimus.graph.diagnostics.sampling.ForensicSource
import optimus.graph.diagnostics.sampling.TaskTracker
import optimus.platform.util.InfoDumpUtils
import optimus.platform.util.Log
import optimus.utils.FileUtils
import optimus.scalacompat.collection._
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.{Option => ArgOption}

import java.io.BufferedReader
import scala.util.Try
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import java.lang.management.ManagementFactory
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.nio.file.Files
import java.nio.file.Paths
import com.sun.tools.attach.VirtualMachine
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.Exceptions
import optimus.platform.util.JExecute
import sun.jvmstat.monitor.MonitoredHost
import sun.jvmstat.monitor.MonitoredVm
import sun.jvmstat.monitor.VmIdentifier
import sun.jvmstat.monitor.event.MonitorStatusChangeEvent
import sun.jvmstat.monitor.event.VmEvent
import sun.jvmstat.monitor.event.VmListener
import sun.jvmstat.perfdata.monitor.protocol.local.LocalMonitoredVm
import sun.tools.attach.HotSpotVirtualMachine

import java.time.Instant
import java.util.Objects
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.unused
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal
import optimus.platform.util.Version
import optimus.utils.PropertyUtils
import sun.jvmstat.monitor.Monitor

class WatcherArgs {
  @ArgOption(name = "--uuid", usage = "Impersonate uuid for diag uploads")
  val uuid: String = ChainedID.root.repr

  @ArgOption(name = "--pid", usage = "Process id to monitor", required = true)
  val pid: Long = 0

  @ArgOption(name = "--env", usage = "Environment for crumb purposes", required = true)
  val env = "qa"

  @ArgOption(name = "--dump", usage = "Generate/upload dump now")
  val dumpNow = false

  @ArgOption(name = "--kill", usage = "Kill process now")
  val killNow = false

  @ArgOption(name = "--listen", usage = "Listen for commands from parent process")
  val listen = false

}

private[heartbeat] object WatcherProcess extends App with InfoDumpUtils with JExecute {

  private val logVmEvents = DiagnosticSettings.getBoolProperty("optimus.watcher.logVmEvents", false)
  private val goodOldGenPct = DiagnosticSettings.getDoubleProperty("optimus.watcher.goodOldGenPct", 99.0)
  private val maxGoodOldGenTimeMs =
    DiagnosticSettings.getLongProperty("optimus.watcher.maxGoodOldGenTimeSec", 300L) * 1000L
  private val livenessTimeoutMs = DiagnosticSettings.getIntProperty("optimus.watcher.liveness.timeout.sec", 60) * 1000L
  private val deleteLogs = !DiagnosticSettings.getBoolProperty("optimus.watcher.retain.diag", true)
  private val doUpload = DiagnosticSettings.getBoolProperty("optimus.watcher.splunk", true)
  private val deleteCores = DiagnosticSettings.getBoolProperty("optimus.watcher.delete.cores", false)
  private val cyclePeriodSec = DiagnosticSettings.getIntProperty("optimus.watcher.cyclePeriod.sec", 5)
  private val verbose = DiagnosticSettings.getBoolProperty("optimus.watcher.verbose", false)
  private val heapDump = DiagnosticSettings.getBoolProperty("optimus.watcher.heap", false)

  val watcherArgs = new WatcherArgs
  val parser = new CmdLineParser(watcherArgs)
  try {
    parser.parseArgument(args: _*)
  } catch {
    case e: CmdLineException =>
      parser.printUsage(System.err)
      System.exit(-1)
  }
  import watcherArgs._

  val isLinux = !DiagnosticSettings.isWindows

  private final case class VM(hsvm: HotSpotVirtualMachine, monitoredHost: MonitoredHost, monitoredVm: LocalMonitoredVm)
  @volatile private var vm: Option[VM] = None

  private def chatter(msg: => String): Unit = if (verbose) log.info(s"$wp: $msg")

  private def attachToParent(): Option[VM] = try {
    log.info(s"Attaching back to $pid")
    val hsvm = VirtualMachine.attach(pid.toString).asInstanceOf[HotSpotVirtualMachine]
    val vmId = new VmIdentifier(pid.toString)
    val monitoredHost = MonitoredHost.getMonitoredHost(vmId)
    // cast to LocalMonitoredVm so that addVmListener links correctly
    val monitoredVm = monitoredHost.getMonitoredVm(vmId, 10000).asInstanceOf[LocalMonitoredVm]
    // Optionally subscribe to and log events
    if (logVmEvents) {
      val vmListener = new VmListener {
        override def monitorStatusChanged(event: MonitorStatusChangeEvent): Unit =
          log.info(event.toString)
        override def monitorsUpdated(event: VmEvent): Unit =
          log.info(event.toString)
        override def disconnected(event: VmEvent): Unit =
          log.info(event.toString)
      }
      monitoredVm.addVmListener(vmListener)
    }
    Some(VM(hsvm, monitoredHost, monitoredVm))
  } catch {
    case e: Throwable =>
      logMessage(s"Unable to attach to $pid", Some(e))
      None
  }

  // Initialize breadcrumbs explicitly from parent configuration
  Breadcrumbs.customizedInit(
    Map("breadcrumb.config" -> env),
    new ZkaContext(ZkaConfig.fromURI(s"zpm://$env.na/optimus").attr(ZkaAttr.KERBEROS, false))
  )

  private val parentHandle = ProcessHandle.of(pid).get
  private val parentExitFuture: CompletableFuture[ProcessHandle] = parentHandle.onExit()
  private var _parentIsDead = false
  private def parentIsDead = _parentIsDead || {
    try {
      val p = parentExitFuture.get(5, TimeUnit.SECONDS)
      _parentIsDead = !p.isAlive
    } catch {
      case _: Exception =>
    }
    _parentIsDead
  }

  // Thread to read from stdin and push to a queue, so that we can listen with a timeout
  private val DONE = "DONE" // for eq comparison
  private val input = new LinkedBlockingQueue[String]()
  private val inputThread = new Thread {
    override def run(): Unit = {
      val in = new BufferedReader(new InputStreamReader(System.in))
      try {
        in.lines().iterator().asScala.foreach(input.put)
      } catch {
        case io: Throwable =>
          log.error(s"Exception reading from stdin", io)
      } finally {
        input.put(DONE)
      }
    }
  }
  inputThread.setDaemon(true)
  inputThread.start()

  @volatile private var stopped = false

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = if (!stopped) {
      log.warn(s"Shutdown hook")
      Try(vm.foreach(vm => vm.monitoredHost.detach(vm.monitoredVm)))
      WatcherProcess.stop()
    }
  })

  private def stop() = {
    stopped = true
    log.info("Shutting down")
    System.exit(0)
  }

  private val wp = Watcher.wp
  private val parentId = ChainedID(uuid)
  private val prefix = s"$wp-$uuid"

  private def logMessage(msg: String, e: Option[Throwable] = None, level: String = "ERROR"): Unit = {
    e match {
      case Some(e) => log.error(s"$wp: $msg", e)
      case None    => log.error(s"$wp: $msg")
    }
    Breadcrumbs.send(
      PropertiesCrumb(
        parentId,
        ForensicSource,
        Properties.logMsg -> s"$wp: msg" :: Properties.logLevel -> "ERROR" :: Properties.ppid -> pid :: Properties.engineRoot -> ChainedID.root.repr :: Properties.stackTrace
          .maybe(e.map(Exceptions.minimizeTrace(_))) :: Properties.logLevel -> level ::
          Version.properties
      ))
  }

  private var dumpIndex = 0
  private def dump(msg: String): Unit = {
    val sb = new StringBuilder()
    dumpIndex += 1

    sb.append(s"$msg\n")
    sb.append(s"Current tasks: $activeTasks\n")

    val pinfo = parentHandle.info()
    sb.append(s"cmd=${pinfo.command().orElse("??")}\n")
    sb.append(s"opts=${pinfo.arguments().orElse(Array.empty).mkString(" ")}\n")

    if (isLinux) {
      execute(sb, timeoutMs = 60000, maxLines = -1, logging = true, "/usr/bin/free")
      execute(sb, timeoutMs = 60000, maxLines = -1, logging = true, "/usr/bin/cat", "/proc/meminfo")
      execute(sb, timeoutMs = 60000, maxLines = -1, logging = true, "/usr/bin/ps", "auxfww")
      // This will fail harmlessly if the pid is gone:
      execute(sb, timeoutMs = 60000, maxLines = -1, logging = true, "/usr/bin/cat", s"/proc/$pid/status")
    }

    if (!parentIsDead) {
      vm.foreach { vm =>
        jexecute(sb, vm.hsvm, "threaddump") // jstacks
        jexecute(sb, vm.hsvm, maxLines = 200, timeoutMs = 30000, logging = true, "inspectheap", "live") // histo
        jexecute(sb, vm.hsvm, "jcmd", "VM.info")
        if (heapDump) {
          val heapFile = FileUtils.tmpPath(s"$wp-heap", "hprof.gz")
          logMessage(s"Dumping heap to $heapFile", level = "WARN")
          jexecute(
            sb,
            vm.hsvm,
            maxLines = -1,
            timeoutMs = 300000, // Hopefully five minutes is enough...
            logging = true,
            "jcmd",
            "GC.heap_dump",
            "-gz=1",
            "-overwrite",
            heapFile.toString
          )
        }
      }
    }

    if (isLinux) {
      // If we found an hs_err file, but the process is still alive, wait a bit for it to terminate.
      if (hs_err(sb)) {
        val waitUntil = System.currentTimeMillis() + 12000L
        while (!parentIsDead && System.currentTimeMillis() < waitUntil)
          Thread.sleep(1000L)
      }
      if (parentIsDead)
        checkCores(sb)
      else
        euStack.foreach { euStack =>
          execute(sb, timeoutMs = 60000, maxLines = -1, logging = true, euStack, "-p", pid)
        }
    }

    val fname = s"$prefix-log-$dumpIndex"
    val s = sb.toString()

    log.info(s"Dumping ${s.length} bytes to $fname")

    for {
      path <- writeToTmpAndLog(fname, ForensicSource, parentId, s, deleteLogs, doLog = true, logger = log)
      rootId <- publishDestinations
    } if (doUpload)
      upload(ForensicSource, rootId, path, deleteUncompressed = true, deleteUploadedFile = false, taskInfo)

  }

  private def hs_err(sb: StringBuilder): Boolean = {
    // Try to find hs_err file
    val pinfo = parentHandle.info()
    val hs_err = Paths.get(
      pinfo
        .arguments()
        .orElse(Array.empty)
        .find(_.startsWith("-XX:ErrorFile="))
        .map(_.stripPrefix("-XX:ErrorFile=").replace("%p", pid.toString))
        .getOrElse(System.getProperty("user.dir") + "/hs_err_pid" + pid.toString + ".log"))

    if (Files.exists(hs_err)) {
      log.info(s"Found $hs_err")
      sb.append(s"Contents of $hs_err:\n")
      sb.append(Files.readString(hs_err))
      true
    } else {
      sb.append(s"No hs_err file found at $hs_err\n")
    }
    false

  }

  private def checkCores(sb: StringBuilder): Unit = {

    var foundCore = false
    def say(s: String): Unit = {
      log.info(s)
      sb.append(s + "\n")
    }
    if (isLinux) euStack.foreach { euStack =>
      try {
        val oneMinuteAgo = System.currentTimeMillis() - 60000L
        say("Looking for core files less than a minute old")
        Files.walk(Paths.get("/var/tmp/cores")).iterator().asScala.foreach { p =>
          var tLast = 0L
          var t = Files.getLastModifiedTime(p).toMillis
          if (t > oneMinuteAgo && Files.isRegularFile(p) && p.getFileName.toString.contains("core")) {
            say(s"$p is recent.  Loop until it's stable.")
            foundCore = true
            do {
              Thread.sleep(1000L)
              tLast = t
              t = Files.getLastModifiedTime(p).toMillis
            } while (t != tLast)
            FileUtils.uncompress(p, deleteOld = deleteCores) match {
              case Success(pUnzipped) =>
                say(s"Extracting binary stack from $pUnzipped")
                execute(sb, timeoutMs = 180000, maxLines = -1, logging = true, euStack, "--core", pUnzipped)
                logMessage(s"Extracted core from $pUnzipped", level = "WARN")
                Try(pUnzipped.toFile.delete())
              case Failure(e) =>
                say(s"uncompress failed: $e")
            }
          }
        }
        if (!foundCore)
          say("No recent core files found")
      } catch {
        case e: Throwable =>
          logMessage(s"Error extracting cores", Some(e))
          say(s"Error extracting cores: $e")
      }
    }
  }

  def kill(): Unit = {
    logMessage(s"Killing parent process $parentHandle")
    parentHandle.destroy()
    if (parentIsDead) return
    log.error("Now trying destroyForcibly")
    parentHandle.destroyForcibly()
    if (parentIsDead) return
    if (isLinux) {
      log.error("Now trying kill -9")
      Runtime.getRuntime.exec(Array("/usr/bin/kill", "-9", parentHandle.pid.toString))
    }
    if (parentIsDead) return
    log.error("Giving up")
  }

  if (dumpNow)
    dump("Dump requested by --dump")

  if (killNow)
    kill()

  private final case class Timer(deadline: Long, interval: Long)

  // Accessed only from main thread
  private val timers = mutable.HashMap.empty[String, Timer]

  // Potentially accessed in shutdown thread too
  private val activeTasks = new mutable.HashSet[ChainedID]()

  @unused
  private def taskInfo = activeTasks.synchronized(Properties.currentlyRunning -> activeTasks.toSeq)
  // Write to one chainedID per active root
  private def publishDestinations: Iterable[ChainedID] = activeTasks.synchronized {
    (activeTasks + parentId).groupBy(_.root).values.map(_.head)
  }

  private val monitors = mutable.HashMap.empty[String, () => Option[Double]]
  private def getDoubleMaker[N](m: Monitor)(implicit n: Numeric[N]): () => Option[Double] = () =>
    Try(n.toDouble(m.getValue.asInstanceOf[N])).toOption

  private def getCounterValue(name: String, default: Option[Double]): Option[Double] = vm.flatMap { vm =>
    val f: () => Option[Double] = monitors.getOrElseUpdate(
      name,
      Option(vm.monitoredVm.findByName(name)) match {
        case Some(m: Monitor) =>
          m.getValue match {
            case _: java.lang.Long    => getDoubleMaker[scala.Long](m)
            case _: java.lang.Double  => getDoubleMaker[scala.Double](m)
            case _: java.lang.Integer => getDoubleMaker[scala.Int](m)
            case _                    => () => default
          }
        case None => () => default
      }
    )
    f()
  }

  // Commands
  private val Register = "register (\\w+) (\\d+)".r // register a ping timer with given timeout
  private val Ping = "ping (\\w+) (\\d+)".r // ping a timer and say when we did it
  private val Delete = "delete (\\w+)".r // delete a timer
  private val Activate = "activate (\\S+)".r // activate a task
  private val Deactivate = "deactivate (\\S+)".r

  private val zeroSome = Some(0.0)
  private val someOne = Some(1.0)

  if (listen) {
    vm = attachToParent()
    logMessage(s"$wp running, vm=$vm", level = "INFO")
    var tLastGoodOg = System.currentTimeMillis()
    while (true) {

      // Check for liveness of parent java process.  Should respond to GC.heap_info
      vm.foreach { vm =>
        chatter("Checking liveness of parent java process")
        val code = jexecute(
          new StringBuilder(),
          vm.hsvm,
          maxLines = -1,
          timeoutMs = livenessTimeoutMs,
          logging = false,
          "jcmd",
          "GC.heap_info")
        if (code == TIMEOUT) {
          val msg = s"Parent java process $pid is unresponsive to GC.heap_info after ${livenessTimeoutMs}ms; killing!"
          logMessage(msg)
          dump("unresponsive")
          kill()
          stop()
        }
      }

      // Check for memory pressure
      // See option gcutil in jdk.jcmd/sun/tools/jstat/resources/jstat_options
      if (vm.isDefined) {
        for {
          oldGenUsed <- getCounterValue("sun.gc.generation.1.space.0.used", None)
          oldGenCapacity <- getCounterValue("sun.gc.generation.1.space.0.capacity", None)
          freq <- getCounterValue("sun.os.hrt.frequency", someOne)
          timeMinor <- getCounterValue("sun.gc.collector.0.time", zeroSome)
          timeFull <- getCounterValue("sun.gc.collector.1.time", zeroSome)
          timeConc <- getCounterValue("sun.gc.collector.2.time", zeroSome)
          numFull <- getCounterValue("sun.gc.collector.1.invocations", zeroSome)
        } {
          val t = System.currentTimeMillis()
          val totalTimeSec = (timeMinor + timeFull + timeConc) / freq
          val oldPctUsed = (1.0 - (oldGenCapacity - oldGenUsed) / oldGenCapacity) * 100.0
          chatter(s"GC time=$totalTimeSec full=$numFull pctOld=$oldPctUsed")
          if (oldPctUsed < goodOldGenPct) tLastGoodOg = t
          // maybe check for increasing time spent in GC too?
          else if ((t - tLastGoodOg) > maxGoodOldGenTimeMs) {
            val msg =
              s"Old gen usage is high: $oldPctUsed > $goodOldGenPct for ${t - tLastGoodOg}ms; gcTime=${totalTimeSec}sec; numFull=$numFull; killing process!"
            logMessage(msg)
            dump(msg)
            kill()
            stop()
          }
        }
      }

      // Check for new command every 5 minutes
      val line = input.poll(cyclePeriodSec, TimeUnit.SECONDS)
      val t = System.currentTimeMillis()

      // Check for expired ping timers
      timers.iterator.minByOption(_._2.deadline).foreach { case (name, Timer(deadline, _)) =>
        chatter(s"Checking timeout for $name")
        if (t > deadline) {
          val msg = s"Ping timeout for $name at ${Instant.ofEpochMilli(deadline)}"
          logMessage(msg)
          dump(msg)
          kill()
          stop()
        }
      }

      if (Objects.nonNull(line)) {
        chatter(s"Got $line")
        line match {
          case DONE if line eq DONE => // want the exact string instance pushed by the reader thread
            log.info("End of input stream")
            dump("Unexpected end of input stream")
            kill()
            stop()
          case "stop" =>
            stop()
          case "dump" =>
            dump("Dump requested by parent process")
          case "kill" =>
            kill()
            stop()
          case Register(name, timeout) =>
            log.info(s"Registering timer $name for every $timeout ms")
            timers.put(name, Timer(t + timeout.toLong, timeout.toLong))
          case Delete(name) =>
            log.info(s"Removing timer $name")
            timers.remove(name)
          case Ping(name, tClient) =>
            log.debug(s"Pinging timer $name")
            // Update to timer.interval from now
            timers.updateWith(name)(_.map(timer => Timer(t + timer.interval, timer.interval))) match {
              case Some(timer) =>
                val lag = System.currentTimeMillis() - tClient.toLong
                if (lag > timer.interval / 10) {

                  log.warn(s"Received ping for $name delayed by $lag ms")
                }
              case None =>
                log.warn(s"Received ping for unregistered $name")
            }
          case Activate(id) =>
            activeTasks.synchronized {
              activeTasks += ChainedID.parse(id)
            }
          case Deactivate(id) =>
            activeTasks.synchronized {
              activeTasks -= ChainedID.parse(id)
            }
          case s =>
            log.warn(s"Unrecognized command: $s")
        }
      }
    }
  }
}

object Watcher extends Log with InfoDumpUtils {

  private[heartbeat] val wp = "WatcherProcess"

  val attachRelatedOpens = Seq(
    "--add-opens=jdk.attach/sun.tools.attach=ALL-UNNAMED",
    "--add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED",
    "--add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED",
    "--add-opens=jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED"
  )

  private def send(cmd: Any*): Unit = watch() match {
    case WatcherChild(p, w) =>
      if (!p.isAlive) {
        log.warn("Watcher process died.  Disabling further attempts to contact it.")
        disable()
      } else {
        try {
          w.write(cmd.mkString(" ") + "\n")
          w.flush()
        } catch {
          case NonFatal(e) =>
            log.warn(s"Unable to send $cmd to watcher: $e")
        }
      }
    case _ =>
  }

  private trait WatcherInfo {
    def running = false
    def writer: Option[BufferedWriter] = None
  }
  private final case object Uninitialized extends WatcherInfo
  private final case class WatcherChild(process: Process, bw: BufferedWriter) extends WatcherInfo {
    override def running = process.isAlive
    override def writer = Some(bw)
  }
  private final case object Disabled extends WatcherInfo

  private val watcher = new AtomicReference[WatcherInfo](Uninitialized)
  private def watch(): WatcherInfo = {
    watcher.updateAndGet {
      case Uninitialized =>
        val pid = ProcessHandle.current().pid
        val moreArgs = PropertyUtils
          .get("optimus.watcher.javaArgs")
          .fold(Seq.empty[String])(_.split(":::").toSeq)

        val moreOpts = PropertyUtils
          .get("optimus.watcher.mainOpts")
          .fold(Seq.empty[String])(_.split(":::").toSeq)

        val nohup = if (!DiagnosticSettings.isWindows) Seq("/usr/bin/nohup") else Seq()

        val watcherProperties = System.getProperties.asScala.collect {
          case (k, v) if k.startsWith("optimus.watcher") && !k.contains(".javaArgs") =>
            s"-D$k=$v"
        }

        val cmd =
          nohup ++
            Seq(
              javaExecutable,
              s"-Dwatcher.ppid=$pid", // early on commandline to be detectable from processhandle in tests
              "-cp",
              System.getProperty("java.class.path"),
              "-Xmx300m", // can be overridden via optimus.watcher.javaArgs
              "-XX:+UseSerialGC", // dramatically less memory overhead
              "-Xrs", // Don't automatically pass on signals from parent
              // "-XX:NativeMemoryTracking=detail",  // uncomment to allow jcmd VM.native_memory_summary
              "-Doptimus.sampling=false",
              "-Dlogback.configurationFile=polite.xml"
            ) ++ moduleArgs ++ attachRelatedOpens ++ watcherProperties ++ moreArgs ++
            Seq(
              "optimus.graph.diagnostics.heartbeat.WatcherProcess",
              "--pid",
              pid.toString,
              "--uuid",
              ChainedID.root.repr,
              "--env",
              Breadcrumbs.getEnv().fold("qa")(_.name),
              "--listen"
            ) ++ moreOpts
        log.info(s"Executing ${cmd.mkString(" ")}")
        try {
          val pb = new ProcessBuilder(cmd: _*)
          pb.redirectError(ProcessBuilder.Redirect.INHERIT)
          pb.redirectOutput(ProcessBuilder.Redirect.INHERIT)
          pb.redirectInput(ProcessBuilder.Redirect.PIPE)
          val p = pb.start()

          log.info(s"Created $wp ${p.pid} watching ${pid}")
          Breadcrumbs.send(
            PropertiesCrumb(
              ChainedID.root,
              ForensicSource,
              Properties.logMsg -> s"Started $wp" :: Properties.cpid -> p.pid :: Version.properties))

          Runtime.getRuntime.addShutdownHook(new Thread {
            override def run(): Unit = disable()
          })

          TaskTracker.registerCallbacks("watcher", id => send("activate", id), id => send("deactivate", id))

          WatcherChild(p, new BufferedWriter(new OutputStreamWriter(p.getOutputStream)))
        } catch {
          case e: Throwable =>
            log.error(s"Unable to register watcher", e)
            Breadcrumbs.send(
              PropertiesCrumb(
                ChainedID.root,
                ForensicSource,
                Properties.logMsg -> s"Unable to start $wp" :: Properties.stackTrace -> Exceptions
                  .minimizeTrace(e) :: Version.properties
              ))
            Disabled
        }
      case w => w
    }
  }

  def disable(): Unit = watcher.updateAndGet { case WatcherChild(p, _) =>
    log.info(s"Deregistering with process watcher")
    if (p.isAlive) {
      Try(send("stop"))
      if (Try(p.onExit().get(1, TimeUnit.SECONDS)).isFailure)
        p.destroyForcibly()
    }
    Disabled
  }

  def enable(): Boolean = watch().running

  def register(key: String, timeoutMs: Long): Unit = send("register", key, timeoutMs)
  def ping(key: String): Unit = send("ping", key, System.currentTimeMillis())
  def delete(key: String): Unit = send("delete", key)

}
