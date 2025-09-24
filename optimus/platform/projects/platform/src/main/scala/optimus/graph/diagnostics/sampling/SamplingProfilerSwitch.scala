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

import msjava.zkapi.ZkaPathContext
import msjava.zkapi.internal.ZkaContext
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.ProfilerSource
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.config.RuntimeConfiguration
import optimus.core.GraphDiagnosticsSource
import optimus.graph.DiagnosticSettings
import optimus.graph.diagnostics.sampling.SamplingProfilerLogger.log
import optimus.utils.OptimusStringUtils
import optimus.utils.zookeeper.ReadOnlyDistributedValue
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.Objects
import scala.util.Random
import scala.util.control.NonFatal
import optimus.platform.util.Version

import java.lang.management.ManagementFactory

object SamplingProfilerSwitch {
  // subnodes will be bas, reg, etc., ...
  private val SP_NODE = "breadcrumbs/sampling-profiler"
  // and each will contain a JSON map like
  // {
  //   "enabled" : true
  // }

  private val spw = "SamplingProfilerSwitch"

  private val requiredCrumbSources = Seq(SamplingProfilerSource)

  private val startedOn = DiagnosticSettings.samplingProfilerStartsOn
  private val defaultOn = DiagnosticSettings.samplingProfilerDefaultOn

  val jvmStart = ManagementFactory.getRuntimeMXBean().getStartTime()

  private final case class SPSwitchConfig(enabled: Option[Boolean], random: Option[Double] = None) {
    def enables = enabled.contains(true)
    def isEmpty = enabled.isEmpty && random.isEmpty

  }
  private implicit val configFormat: RootJsonFormat[SPSwitchConfig] = jsonFormat2(SPSwitchConfig)
  @volatile private var prevConfig = SPSwitchConfig(Some(startedOn))
  private val OFF = SPSwitchConfig(Some(false))
  private val EMPTY = SPSwitchConfig(None)
  private val arg = DiagnosticSettings.samplingProfilerArg

  private def warn(msg: String): Unit = {
    log.warn(msg)
    Breadcrumbs.warn(
      ChainedID.root,
      PropertiesCrumb(
        _,
        ForensicSource,
        Properties.logMsg -> s"$spw: $msg" :: Properties.severity -> "WARN" :: Version.properties)
    )
  }

  private def maybeShutDown(path: String)(config: SPSwitchConfig): Boolean = {
    def explain = s"sampling.profiler=${arg}, ZK $path=${config.toJson}, startedOn=$startedOn, defaultOn=$defaultOn"
    def crumb(action: String): Unit = warn(s"$action, $explain")
    def turnOff(): Boolean = {
      val dt = SamplingProfiler.instance().fold(-1L)(System.currentTimeMillis() - _.startTime)
      crumb(s"Shutting off and permanently disabling SampingProfiler after ${dt}ms")
      SamplingProfiler.shutdownNow()
      true
    }
    def turnOn(): Boolean = {
      val dt = System.currentTimeMillis() - jvmStart
      crumb(s"Starting SamplingProfiler ${dt}ms after jvm start ")
      SamplingProfiler.start()
      false
    }
    def stayOff(): Boolean = {
      crumb("Not starting SamplingProfiler")
      true
    }
    def stayOn(): Boolean = {
      crumb("Leaving SamplingProfiler running")
      false
    }
    def beOn(): Boolean = if (startedOn) stayOn() else turnOn()
    def beOff(): Boolean = if (startedOn) turnOff() else stayOff()

    prevConfig = config
    if (
      config.enables
      || config.random.exists(_ > Random.nextDouble())
      || (config.isEmpty && defaultOn)
    )
      beOn()
    else beOff()
  }

  private var distributedConfig: ReadOnlyDistributedValue[SPSwitchConfig] = _

  /*
   * SamplingProfiler may already be turned on at this point (via static initialization), in which case ZK
   * configuration will just shut it off "fairly quickly."
   * See configuration table in DiagnosticSettings
   */
  def configure(rtc: RuntimeConfiguration, rootContext: ZkaContext): Unit = SamplingProfiler.synchronized {
    if (DiagnosticSettings.samplingProfilerZkConfigurable && !SamplingProfiler.configured) {
      val sinceStart = System.currentTimeMillis() - jvmStart
      if (sinceStart > SamplingProfiler.configTimeoutSec * 1000L) {
        val msg =
          s"Not configured until ${sinceStart}ms > ${SamplingProfiler.configTimeoutSec}s, startedOn=$startedOn, defaultOn=$defaultOn"
        (defaultOn, SamplingProfiler.running) match {
          case (false, true) =>
            warn(s"$msg; Shutting off SamplingProfiler")
            SamplingProfiler.shutdownNow()
          case (false, false) =>
            warn(s"$msg; leaving SamplingProfiler off")
          case (true, false) =>
            warn(s"$msg: Turning SamplingProfiler on")
            SamplingProfiler.start()
          case (true, true) =>
            warn(s"$msg: leaving SamplingProfiler on")
        }
        return
      }
      log.info("Launching configuration thread")
      SamplingProfiler.configured = true
      val configThread = new Thread {
        override def run(): Unit = {
          log.info("Starting configuration")
          val zkc: ZkaPathContext = ZkaContext.contextForSubPath(rootContext, SP_NODE)
          try {
            def addChildNodeIfExists(original: String, child: String) = {
              val provisional = s"$original/$child"
              if (Objects.nonNull(zkc.exists(provisional)))
                provisional
              else {
                warn(s"Path $zkc ${zkc.paths(provisional)} doesn't exist; falling back to $original")
                original
              }
            }
            val configNode = {
              // Start with base environment, e.g. reg, bas, prod2
              var ret = rtc.mode
              // Optionally an optimal subdir from the optimus.sampling property.
              if (arg.contains('/')) {
                val sub = arg.split('/')(1)
                ret = addChildNodeIfExists(ret, sub)
              }
              // Check for existence of appId subnode as well
              rtc.get("optimus.dsi.appid").foreach { appId =>
                ret = addChildNodeIfExists(ret, appId.toString)
              }
              ret
            }
            val path = s"/$SP_NODE/$configNode"

            def parse(data: Array[Byte]): SPSwitchConfig = {
              try {
                OptimusStringUtils.charsetAwareToString(data, true).parseJson.convertTo[SPSwitchConfig]
              } catch {
                case NonFatal(e) =>
                  warn(s"$e: Unable to parse data from $path $data")
                  prevConfig
              }
            }
            val checkMsg = s"Checking SamplingProfiler configuration at $path"
            // Ensure that SP crumbs are going to kafka by sending out probes and flushing.
            // There are so many possible configurations and filters that could block us that
            // that this is the only reliable way to find out.
            requiredCrumbSources.foreach { source =>
              Breadcrumbs.info(
                ChainedID.root,
                PropertiesCrumb(
                  _,
                  source,
                  Properties.logMsg -> checkMsg ::
                    Properties.crumbplexerIgnore -> "SamplingProfilerSwitch" :: Version.properties))
            }
            Breadcrumbs.flush()
            SamplingProfilerSource.flush()
            // If either source is blocked, we might as well shut off profiling, since the data would never
            // escape the process.
            if (requiredCrumbSources.exists(_.sentCount == 0)) {
              val noCrumbMsg =
                s"Shutting off SamplingProfiler since we are unable to publish crumbs to ${requiredCrumbSources.mkString(", ")}! " +
                  requiredCrumbSources.map(s => s"$s -> ${s.sentCount}").mkString(", ")
              log.warn(noCrumbMsg)
              // Send the warning somewhere other than PROF or SP...
              Breadcrumbs.warn(
                ChainedID.root,
                PropertiesCrumb(_, ForensicSource, Properties.logMsg -> noCrumbMsg :: Version.properties))
              maybeShutDown(path)(OFF)
            } else {
              log.info(checkMsg)
              if (Objects.isNull(zkc.exists(configNode))) {
                warn(s"Path $zkc ${zkc.paths(configNode)} doesn't exist in ZK; treating as empty config.")
                maybeShutDown(path)(EMPTY)
              } else {
                val data = zkc.getNodeData(configNode)
                val cfg = parse(data)
                maybeShutDown(path)(cfg)
              }
            }
          } catch {
            case NonFatal(e) =>
              warn(s"Unable to read SP configuration: $e")
              if (Objects.nonNull(distributedConfig)) {
                distributedConfig.close()
                distributedConfig = null
              }
              maybeShutDown("UnknownPath")(EMPTY)
          } finally {
            zkc.close()
          }
          log.info("Configuration complete")
        }
      }
      configThread.setName("SamplingProfilerSwitch")
      configThread.setDaemon(true)
      configThread.start()
      log.info("Launched configuration thread")
    }
  }
}
