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

import optimus.buildtool.cache.silverking.SilverKingConfig.ZookeeperLookup
import optimus.dal.silverking.SilverKingPlantConfig

class ObtConfigException(msg: String) extends Exception(msg)

object ObtConfigHealthCheck {

  def checkSwitch(switch: Variable[Map[OperationType, String]]): Unit = {
    val state = switch.state
    if (state.nonEmpty) {
      throw new ObtConfigException(
        s"Cluster not enabled for ${state.keySet}. If there is not an upgrade ongoing, re-enable this in ZK ASAP")
    }
  }

  def assertEnabled(skPlantConfig: SilverKingPlantConfig): Unit = {
    val zkl = ZookeeperLookup(skPlantConfig)
    val clusterType = ClusterType.forLookup(zkl)
    checkSwitch(ZookeeperSwitch(zkl, clusterType, None))
  }
}
