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
package optimus.platform.inputs

import optimus.config.NodeCacheConfigs
import optimus.platform.inputs.registry.ProcessGraphInputs
import optimus.config.OptconfProvider.fromContent
import optimus.platform.inputs.registry.ProcessGraphApplicators
import optimus.config.OptconfProvider
import optimus.graph.diagnostics.trace.OGEventsObserver
import optimus.platform.inputs.ProcessState.mergeStateWithPropFile
import optimus.platform.inputs.loaders.Loaders
import optimus.platform.inputs.loaders.OptimusNodeInputStorage
import optimus.graph.diagnostics.gridprofiler.Level.Level
import optimus.graph.diagnostics.gridprofiler.Level

import java.util.Properties
import scala.jdk.CollectionConverters._

object GraphInputConfiguration {

  /**
   * Apply cache configuration to node properties on the fly. This should always be followed by resetCachePolicies!
   * Remember this affects the JVM you're running in. If you can, use --config-graph instead
   * @param optconfPaths
   *   paths to optconf files to apply (order matters - in case of conflicting configuration for a given property, last
   *   one wins). Classpath syntax (e.g. classpath:/config/...) is allowed
   */
  def configureCachePolicies(optconfPaths: Seq[String]): Unit =
    if ((optconfPaths ne null) && ProcessGraphInputs.ConfigOverride.currentValueOrThrow().isEmpty)
      ProcessState.changeValueOfInputTo(
        ProcessGraphInputs.ConfigGraph,
        optconfPaths.flatMap(NodeCacheConfigs.verifyPathAndCreateProvider).asJava)

  def resetCachePolicies(): Unit = ProcessState.removeInput(ProcessGraphInputs.ConfigGraph)

  def setProfileCsvFolder(dir: String): Unit =
    ProcessState.changeValueOfInputTo(ProcessGraphInputs.ProfilerCsvFolder, dir)

  def loadInputsFromFileIntoState(p: Properties): Unit = mergeStateWithPropFile(
    Loaders.javaProperties(OptimusNodeInputStorage.empty, p).underlying().processSINodeInputMap)

  def setHotspotsFilter(customHotspots: Array[String]): Unit =
    ProcessState.changeValueOfInputTo(
      ProcessGraphInputs.HotspotsFilter,
      ProcessGraphInputs.parseCustomHotspotsFilter(customHotspots))

// TODO (OPTIMUS-55093): remove startup content option and allow only for test
  def configureCachePoliciesFromContent(content: String, startupContent: String = null): Unit =
    configureCachePoliciesFromContents(Seq(content), startupContent)

  def configureCachePoliciesFromContents(content: Seq[String], startupContent: String = null): Unit = {
    val startupProvider = Option(startupContent).map(fromContent(_))
    configureCachePoliciesFromProviders(content.map(fromContent(_)), startupProvider)
  }

  def onStartupComplete(): Unit =
    if (ProcessGraphInputs.StartupConfig.currentValueOrThrow().nonEmpty) {
      ProcessState.removeInput(ProcessGraphInputs.StartupConfig)
      ProcessState.reapplyApplicator(ProcessGraphApplicators.OptconfApplicator)
    }

  def configureCachePoliciesFromProviders(
      optconfPaths: Seq[OptconfProvider],
      startupOptconf: Option[OptconfProvider] = None): Unit = {
    // get rid of this because we don't care about the ignore in this case
    ProcessState.removeInput(ProcessGraphInputs.IgnoreGraphConfig)
    val optconfPathIsEmpty = optconfPaths.isEmpty
    if (optconfPathIsEmpty) ProcessState.removeInput(ProcessGraphInputs.ConfigGraph)
    val inputsToChange =
      if (optconfPathIsEmpty)
        Seq.empty
      else
        Seq((ProcessGraphInputs.ConfigGraph, optconfPaths.asJava))

    if (startupOptconf.isEmpty) {
      ProcessState.removeInput(ProcessGraphInputs.StartupConfig)
      ProcessState.changeValueOfInputsTo(inputsToChange)
    } else
      ProcessState.changeValueOfInputsTo(
        inputsToChange ++
          Seq((ProcessGraphInputs.StartupConfig, startupOptconf.get)))
  }

  private[optimus] def DEBUGGER_ONLY_disableXSFT(): Unit =
    ProcessState.changeValueOfInputTo(ProcessGraphInputs.EnableXSFT, java.lang.Boolean.FALSE)

  def setTraceMode(name: String): Unit =
    setTraceMode(
      if (name == null) Level.NONE
      else Level.withName(name.toUpperCase))

  def setTraceMode(newObserver: OGEventsObserver): Unit = setTraceMode(Level.withName(newObserver.name.toUpperCase))

  def setTraceMode(lvl: Level): Unit = ProcessState.changeValueOfInputTo(ProcessGraphInputs.ProfileLevel, lvl)
}
