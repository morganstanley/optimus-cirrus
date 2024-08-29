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
package optimus.dsi.cli

object ZkPaths {
  //
  // Hard-coded ZooKeeper path locations
  //
  private[dsi] val ZK_GLOBALCONFIG_NODE = "/_"
  private[optimus] val ZK_BROKERCONFIG_NODE = "/brokerconfig"
  private[optimus] val ZK_BROKERCONFIGV2_NODE = "/brokerconfigv2"
  private[optimus] val ZK_BROKERS_NODE = "/brokers"
  private[optimus] val ZK_SESSION_NODE = "/session"
  private[optimus] val ZK_DAL_SERVICE_NODE = "/dalServices"
  //
  // Hard-coded XPATH locations inside XML config
  //
  private[dsi] val ZK_XPATH_ENVIRONMENTS = "/config/optimus/dsimappings/global"
  private[optimus] val ZK_XPATH_MONGOS = "/config/optimus/dsimappings/mongodb/plants"
  private[dsi] val ZK_XPATH_POSTGRES = "/config/optimus/dsimappings/mongodb/plants"
  private[dsi] val ZK_XPATH_BROKERCONFIG_ENTITLEMENTS = "/config/optimus/dsi/entitlement"
  private[dsi] val ZK_XPATH_BROKERCONFIG_ENTITLEMENTS_PARENT = "/config/optimus/dsi"
  private[dsi] val ZK_XPATH_BROKERCONFIG_MQ = "/config/optimus/dsi/MQ"
  private[dsi] val ZK_XPATH_BROKERCONFIG_REPLICATION = "/config/optimus/dsi/replication"
  private[dsi] val ZK_XPATH_BROKERCONFIG_THROTTLING = "/config/optimus/dsi/throttling"
  private[dsi] val ZK_XPATH_BROKERCONVIGV2_SHARD = "/metroplex/shards/shard"
}
