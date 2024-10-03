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

import optimus.breadcrumbs.ChainedID
import optimus.config.scoped.ScopedSchedulerPlugin
import optimus.graph.diagnostics.gridprofiler.GridProfiler
import optimus.platform._
import optimus.observability._
import optimus.platform.inputs.loaders.FrozenNodeInputMap

import java.lang.ref.WeakReference

object SIParams {
  private val emptyWeakRef = new WeakReference[Scheduler](null)
}

/**
 * The "scenario independent" parameters of ScenarioStack which do not affect cache lookup
 */
final case class SIParams(
    nodeInputs: FreezableNodeInputMap = FrozenNodeInputMap.empty,
    cancelScope: CancellationScope = NullCancellationScope,
    @transient batchScope: BatchScope = new BatchScope,
    progressReporter: ProgressReporter = null,
    // profiler block IDs have no meaning across JVMs so need reinitializing in readResolve
    @transient profileBlockID: Int = OGTrace.BLOCK_ID_UNSCOPED,
    parentTrackingNodeID: ChainedID = null,
    trackingNodeID: ChainedID = null,
    @transient seqOpID: Long = 0L,
    progressTracker: ProgressTracker = null,
    jobConfiguration: JobConfiguration = MissingJobConfiguration,
    scopedPlugins: Map[NodeTaskInfo, ScopedSchedulerPlugin] = null,
    // note: not included in hashcode, will be reset if it didn't match
    @transient scheduler: WeakReference[Scheduler] = SIParams.emptyWeakRef
) {

  private[optimus] def markPluginNodeTaskInfos(): Unit = if (scopedPlugins ne null) {
    scopedPlugins.foreach { case (nti, _) => nti.markAsShouldLookupPlugin() }
  }

  def resolvePlugin(info: NodeTaskInfo): SchedulerPlugin =
    if (scopedPlugins eq null) null
    else {
      scopedPlugins.get(info) match {
        case Some(entry) =>
          val enablePluginTagKey = entry.enablePluginTagKey
          if ((enablePluginTagKey eq null) || nodeInputs.contains(enablePluginTagKey)) entry.schedulerPlugin
          else null
        case None => null
      }
    }

  // note that we can't just use @stable because we're in core, so the entityplugin doesn't apply to this code, so we
  // write cached hashCode by hand. Note also that the default case class equals is fine so we don't write that.
  private[this] var hashCodeCache: Int = 0
  override def hashCode(): Int = {
    if (hashCodeCache != 0) hashCodeCache
    else {
      var h = nodeInputs.##
      h = h * 37 + cancelScope.##
      h = h * 37 + batchScope.##
      h = h * 37 + progressReporter.##
      h = h * 37 + profileBlockID
      h = h * 37 + parentTrackingNodeID.##
      h = h * 37 + trackingNodeID.##
      h = h * 37 + seqOpID.##
      h = h * 37 + progressTracker.##
      h = h * 37 + jobConfiguration.##
      h = h * 37 + scopedPlugins.##
      hashCodeCache = h
      h
    }
  }

  // noinspection ScalaUnusedSymbol
  private def readResolve: AnyRef = {
    val newId =
      nodeInputs
        .getInput(GridProfiler.Tag)
        .map(t => GridProfiler.scopeToBlockID(t.scope))
        .getOrElse(OGTrace.BLOCK_ID_UNSCOPED)
    copy(profileBlockID = newId, batchScope = new BatchScope)
  }
}
