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

import msjava.zkapi.internal.ZkaContext
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Crumb.ProfilerSource
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.config.RuntimeConfiguration
import optimus.core.GraphDiagnosticsSource
import optimus.graph.DiagnosticSettings
import optimus.platform.util.Log
import optimus.utils.OptimusStringUtils
import optimus.utils.PropertyUtils
import optimus.utils.zookeeper.ReadOnlyDistributedValue
import spray.json.DefaultJsonProtocol._
import spray.json._

import java.util.Objects
import scala.util.Random
import scala.util.control.NonFatal
import optimus.platform.util.Version

object SamplingProfilerSwitch extends Log {
  // subnodes will be bas, reg, etc., ...
  private val SP_NODE = "breadcrumbs/sampling-profiler"
  // and each will contain a JSON map like
  // {
  //   "enabled" : true
  // }

  private val requiredCrumbSources = Seq(SamplingProfilerSource)

  private final case class SPSwitchConfig(enabled: Option[Boolean], random: Option[Double] = None) {
    def enables = enabled.contains(true)
    def disables = enabled.contains(false)
    def agnostic = enabled.isEmpty

  }
  private implicit val configFormat: RootJsonFormat[SPSwitchConfig] = jsonFormat2(SPSwitchConfig)
  @volatile private var prevConfig = SPSwitchConfig(Some(DiagnosticSettings.samplingProfilerDefaultOn))
  private val OFF = SPSwitchConfig(Some(false))

  private def maybeShutDown(path: String)(config: SPSwitchConfig): Boolean = {
    def explain = s"sampling.profiler=${DiagnosticSettings.samplingProfilerArg}, ZK $path=${config.toJson}"
    if (config.enables) {
      log.info(s"SP explicitly enabled: $explain")
      false
    } else if (config.agnostic && (DiagnosticSettings.samplingProfilerDefaultOn)) {
      log.info(s"SP staying on by default: $explain")
      false
    } else if (config.random.exists(_ > Random.nextDouble())) {
      log.info(s"SP staying on probabilistically: $explain")
      false
    } else {
      // don't really care about race condition - it's only to avoid multiple shutdown warnings
      prevConfig = config
      val msg = s"Permanently disabling SP: $explain"
      log.warn(msg)
      Breadcrumbs.warn(
        ChainedID.root,
        PropertiesCrumb(
          _,
          ProfilerSource,
          Properties.logMsg -> msg :: Properties.severity -> "WARN" :: Version.properties)
      )
      SamplingProfiler.shutdownNow()
      true
    }
  }

  private var distributedConfig: ReadOnlyDistributedValue[SPSwitchConfig] = _

  val listenForUpdates = PropertyUtils.get("optimus.sampling.switch.listen", false)
  /*
   * SamplingProfiler may already be turned on at this point (via static initialization), in which case ZK
   * configuration will just shut it off "fairly quickly."
   *                             {enabled:true}   {enabled:false}   {} or missing
   *   optimus.sampling=true     Stay on          Shut off          Stay on
   *                   =config   Stay on          Shut off          Shut off
   *                   =force    Stay on          Stay on           Stay on
   *                   =false    Not even initialized, no matter what the ZK setting
   */
  def configure(rtc: RuntimeConfiguration, rootContext: ZkaContext): Unit = SamplingProfiler.synchronized {
    if (DiagnosticSettings.samplingProfilerZkConfigurable && !SamplingProfiler.configured) {
      log.info("Launching configuration thread")
      SamplingProfiler.configured = true
      val configThread = new Thread {
        override def run(): Unit = {
          log.info("Starting configuration")
          try {
            val mode = rtc.mode
            val path = s"/$SP_NODE/$mode"

            def parse(data: Array[Byte]): SPSwitchConfig = {
              try {
                OptimusStringUtils.charsetAwareToString(data, true).parseJson.convertTo[SPSwitchConfig]
              } catch {
                case NonFatal(e) =>
                  log.warn(s"$e: Unable to parse data from $path $data")
                  prevConfig
              }
            }
            val checkMsg =
              (if (listenForUpdates) "Listening" else "Checking") + s" for SamplingProfiler shutdown at $path"
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
                PropertiesCrumb(_, GraphDiagnosticsSource, Properties.logMsg -> noCrumbMsg :: Version.properties))
              maybeShutDown(path)(OFF)
            } else {
              log.info(checkMsg)
              if (listenForUpdates) {
                val curator = rootContext.getCurator
                distributedConfig = new ReadOnlyDistributedValue(curator, path, parse) {
                  override def onNodeChange(v: Option[SPSwitchConfig]): Unit = v.foreach { cfg =>
                    if (maybeShutDown(path)(cfg))
                      distributedConfig.close()
                  }
                }
                val didShutDown = distributedConfig.value match {
                  case Some(cfg) => maybeShutDown(path)(cfg)
                  case None =>
                    !DiagnosticSettings.samplingProfilerDefaultOn &&
                    maybeShutDown(path)(OFF)
                }
                if (didShutDown) {
                  val closeable = distributedConfig
                  distributedConfig = null
                  closeable.close()
                }
              } else {
                val zkc = ZkaContext.contextForSubPath(rootContext, SP_NODE)
                val data = zkc.getNodeData(mode)
                val cfg = parse(data)
                maybeShutDown(path)(cfg)
              }

            }
          } catch {
            case NonFatal(e) =>
              log.warn(s"Unable to read SP configuration: $e")
              if (Objects.nonNull(distributedConfig)) {
                distributedConfig.close()
                distributedConfig = null
              }
              if (!DiagnosticSettings.samplingProfilerDefaultOn)
                maybeShutDown("UnknownPath")(OFF)
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
