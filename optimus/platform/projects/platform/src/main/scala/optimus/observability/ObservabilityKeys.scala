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
package optimus.observability

import optimus.breadcrumbs.crumbs.KnownProperties
import optimus.breadcrumbs.crumbs.Properties.Elems
import optimus.graph.JobForwardingPluginTagKey
import optimus.graph.NodeTask
import optimus.graph.NonBatchingPluginTag
import spray.json.DefaultJsonProtocol
import spray.json.RootJsonFormat

/** Plugin tag key to hold the properties of the currently executing parent job, if any such exists. */
private[optimus] case object AtJobPropertiesPluginTagKey
    extends JobForwardingPluginTagKey[AtJobProperties]
    with NonBatchingPluginTag

final case class AtJobProperties(uuid: String, chainedId: String, pinfo: String, dal: String)
object AtJobProperties extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[AtJobProperties] = jsonFormat4(AtJobProperties.apply)
}

object ObservabilityKeys extends KnownProperties {
  import AtJobProperties._

  // the @job properties
  val atJob: ObservabilityKeys.EnumeratedKeyRef[AtJobProperties] = prop[AtJobProperties]("atjob")

  def extractFrom(ntsk: NodeTask): Elems =
    ntsk.scenarioStack
      .findPluginTag(AtJobPropertiesPluginTagKey)
      .map(atJob -> _) :: Elems.Nil
}
