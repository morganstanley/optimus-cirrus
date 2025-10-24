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
package optimus.dist

import optimus.breadcrumbs.crumbs.KnownProperties
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.graph.JobForwardingPluginTagKey
import optimus.graph.Node
import optimus.graph.NodeTask
import optimus.platform.BatchLevelPluginTagKey
import optimus.scalacompat.collection._

object DistPluginTags extends KnownProperties {
  import spray.json.DefaultJsonProtocol._

  private val batchedNodes = prop[Map[String, String]]("batched_nodes")

  /**
   * @param jobId the job id of the distributed node currently executing in the engine
   * @param taskId the task id of the distributed node currently executing in the engine
   * @param gridProfilerID a string concatenating job and task id of the currently grid profiler tracked distributed node. this could be identical to [[jobId]] and [[taskId]], but differs if the distributed node was stolen by recursive distribution.
   */
  final case class JobTaskIdTag(jobId: String, taskId: String, gridProfilerID: String) {

    /** @return a cache key used to identify the task that was distributed to the current engine */
    private[dist] def cacheKey: (String, String) = (jobId, taskId)
    def fullyQualified = s"$jobId.$taskId"
  }
  object JobTaskIdTag extends JobForwardingPluginTagKey[JobTaskIdTag] with BatchLevelPluginTagKey[JobTaskIdTag] {
    def apply(jobId: String, taskId: String): JobTaskIdTag = JobTaskIdTag(jobId, taskId, s"$jobId.$taskId")
  }

  /** Exposes [[optimus.graph.NodeTaskInfo]] to distribution */
  final case class GroupingNodeBatcherSchedulerPluginTag(nodes: Map[String, Int])
  object GroupingNodeBatcherSchedulerPluginTag
      extends JobForwardingPluginTagKey[GroupingNodeBatcherSchedulerPluginTag] {

    def apply(nodes: Seq[Node[_]]): GroupingNodeBatcherSchedulerPluginTag =
      GroupingNodeBatcherSchedulerPluginTag(
        nodes.groupBy(_.executionInfo).map { case (k, v) => k.nodeName.toString -> v.size })
  }

  def extractFrom(ntsk: NodeTask): Elems =
    ntsk.scenarioStack
      .findPluginTag(GroupingNodeBatcherSchedulerPluginTag)
      .map(v => Elems(batchedNodes -> v.nodes.mapValuesNow(_.toString)))
      .getOrElse(Elems.Nil)
}
