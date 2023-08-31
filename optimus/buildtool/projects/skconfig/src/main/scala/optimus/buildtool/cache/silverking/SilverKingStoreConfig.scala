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
package optimus.buildtool.cache.silverking

object SilverKingStoreConfig {
  private[silverking] val prefix = "optimus.buildtool.silverking"
  // Some(True): always enabled
  // Some(false): always disabled
  // None: dependent on zookeeper state
  val enabled: Option[Boolean] = sys.props.get(s"$prefix.enabled").map(_.toBoolean)

  private[cache] def clusterStr(clusterType: ClusterType) = s"Distributed artifact cache (SilverKing $clusterType)"

}
