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
package optimus.graph

import msjava.tools.util.MSProcess
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.breadcrumbs.{Breadcrumbs, ChainedID}
import optimus.breadcrumbs.crumbs._
import optimus.dist.breadcrumbs.DistProperties
import optimus.logging.LoggingInfo
import optimus.platform.EvaluationContext

private[optimus] object GraphCrumbSource extends Crumb.Source { override val name = "OG" }

private[optimus] object GraphEvents {
  val Stalling = Events.GraphStalling
  val Stalled = Events.GraphStalled
  val StallDetected = Events.StallDetected

  private lazy val host: String = LoggingInfo.getHost
  private lazy val pid: Long = MSProcess.getPID
  private def getEnv: String = {
    // we're being very defensive about possible nulls here because this code is
    // sometimes called when the environment is in weird states
    if (!EvaluationContext.isInitialised) "N/A"
    else
      try {
        // don't track the access because this is only used for diagnostics
        EvaluationContext.scenarioStack.ssShared.environmentWithoutTrackingAccess.config.runtimeConfig.env
      } catch {
        case _: Throwable => "N/A"
      }
  }
  private val maxStalledRequests = Settings.detectStallMaxRequests

  private[graph] def publishGraphStallingCrumb(stallTime: Long, stallSource: SchedulerStallSource): Unit =
    publishGraphStallingEventCrumb(GraphEvents.Stalling, stallTime, stallSource)
  private[graph] def publishGraphStalledCrumb(stallTime: Long, stallSource: SchedulerStallSource): Unit =
    publishGraphStallingEventCrumb(GraphEvents.Stalled, stallTime, stallSource)

  private def publishGraphStallingEventCrumb(
      event: Events.EventVal,
      stallTime: Long,
      stallSource: SchedulerStallSource): Unit = {
    val stallInfos = stallSource.awaitedTasks.map { at =>
      GraphStallInfo(Option(at.endOfChainPlugin()), at.endOfChainTaskInfo())
    }
    val requestsStallInfo =
      stallInfos.flatMap(s => s.requestsStallInfo.map(ReasonRequestsStallInfo(s.pluginType.name, _)))
    if (requestsStallInfo.nonEmpty) {
      val distElems =
        stallSource.jobTaskId.map(v => Elems(DistProperties.jobId -> v.jobId, DistProperties.taskId -> v.taskId))
      Breadcrumbs.info(ChainedID.root, EventCrumb(_, GraphCrumbSource, event))
      Breadcrumbs.info(
        ChainedID.root,
        EventPropertiesCrumb(
          _,
          GraphCrumbSource,
          distElems ::: Elems(
            Properties.env -> getEnv,
            Properties.host -> host,
            Properties.pid -> pid,
            Properties.event -> event.name,
            Properties.duration -> stallTime / 1000000,
            Properties.reasonRequestsStallInfo -> requestsStallInfo
          )
        )
      )
    }
  }

  private[graph] def publishStallDetectedCrumb(stallLogFile: String) = {
    Breadcrumbs.info(ChainedID.root, EventCrumb(_, GraphCrumbSource, GraphEvents.StallDetected))
    Breadcrumbs.info(
      ChainedID.root,
      EventPropertiesCrumb(
        _,
        GraphCrumbSource,
        Properties.env -> getEnv,
        Properties.host -> host,
        Properties.pid -> pid,
        Properties.event -> GraphEvents.StallDetected.name,
        Properties.file -> stallLogFile
      )
    )
  }
}
