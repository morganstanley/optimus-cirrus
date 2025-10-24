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
package optimus.platform

import msjava.tools.util.MSProcess
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.BreadcrumbsSendLimit.OnceByCrumbEquality
import optimus.breadcrumbs.crumbs.CrumbNodeType
import optimus.breadcrumbs.crumbs.NameCrumb
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.graph.DiagnosticSettings
import optimus.graph.Node
import optimus.graph.NodeTaskInfo
import optimus.graph.NodeTrace
import optimus.graph.OGSchedulerContext
import optimus.logging.LoggingInfo

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Map

/** Per thread 'entry' node, really internal to graph */
private[optimus] class StartNode(ss: ScenarioStack) extends Node[Unit] {
  super.setId(OGSchedulerContext.NODE_ID_ROOT) // Reset as root-of-graph marker
  initAsCompleted(ss)
  // Whenever someone will try to add extended info, we will just drop it on the floor
  _xinfo = optimus.graph.SinkNodeExtendedInfo
  if (DiagnosticSettings.traceAvailable) NodeTrace.registerRoot(this)
  if (Breadcrumbs.collecting) {
    Breadcrumbs.info(
      OnceByCrumbEquality,
      ss.trackingNodeID,
      PropertiesCrumb(
        _,
        Properties.config ->
          // the config is only used for sending diagnostics so we don't track access
          Option(ss.ssShared.environmentWithoutTrackingAccess.config)
            .flatMap(c => Option(c.runtimeConfig))
            .map(_.propertyMap)
            .getOrElse(Map.empty),
        Properties.pid -> MSProcess.getPID,
        Properties.host -> LoggingInfo.getHost,
        Properties.logFile -> LoggingInfo.getLogFile,
        Properties.agents -> AgentInfo.agentInfo().asScalaUnsafeImmutable,
        Properties.user -> LoggingInfo.getUser
      )
    )
    Breadcrumbs.info(OnceByCrumbEquality, ss.trackingNodeID, new NameCrumb(_, "NullNode", CrumbNodeType.NullNode))
  }

  override def result: Unit = ()
  override def executionInfo: NodeTaskInfo = NodeTaskInfo.Start
}
