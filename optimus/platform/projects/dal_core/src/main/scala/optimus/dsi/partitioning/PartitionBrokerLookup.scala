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
package optimus.dsi.partitioning

import msjava.slf4jutils.scalalog.getLogger
import optimus.config.OptimusConfigurationException
import optimus.config.RuntimeConfiguration
import optimus.dsi.partitioning.PartitionBrokerLookup.BrokerAddress
import optimus.dsi.partitioning.PartitionBrokerLookup.PartitionToBrokerMap

final case class PartitionBrokerLookup(
    private val writeBrokerMap: PartitionBrokerLookup.PartitionToBrokerMap,
    private val pubsubBrokerMap: Map[BrokerAddress, PartitionToBrokerMap]) {
  import PartitionBrokerLookup._

  /**
   * In case of write brokers, they are only deployed in one region, so it's a simple map.
   */
  def getWriteBroker(partition: NamedPartition, defaultAddress: BrokerAddress): BrokerAddress =
    writeBrokerMap.getOrElse(partition, defaultAddress)

  /**
   * In case of pubsub, there could be regional level deployments where for e.g., region "ny" has separate pubsub
   * brokers for some or all partitions, while region "ln" doesn't. Now there's no concept of region as far as
   * client-side config is concerned and it's equivalent to broker address in "replica" (or other) URIs.
   *
   * Assume following replica URIs configured for "ny" and "ln":
   *   - "ny" -> replica://devny?pubsub=devpsny
   *   - "ln" -> replica://devln?pubsub=devpsln
   *
   * Following would be the configuration for pubsub partition lookup where "ny" has separate pubsub brokers:
   * {{{
   *   <pubsubpartitions>
   *     <devpsny>
   *       <partition name="partition1" lookup="devpsp1ny"/>
   *       <partition name="partition2" lookup="devpsp2ny"/>
   *     </devpsny>
   *   </pubsubpartitions>
   * }}}
   *
   *   - Lookup for "devpsln" for all partitions will resolve to "devpsln".
   *   - Lookup for "devpsny" for partition "p1" will resolve to "devpsp1ny".
   *   - Lookup for "devpsny" for partition "p2" will resolve to "devpsp2ny".
   *   - Lookup for "devpsny" for partition "default" will resolve to "devpsny".
   *   - Lookup for "devpsny" for partition "other" will resolve to "devpsny".
   */
  def getPubSubBroker(partition: NamedPartition, defaultAddress: Option[BrokerAddress]): Option[BrokerAddress] = {
    defaultAddress
      .map { addr =>
        pubsubBrokerMap
          .get(addr)
          .flatMap(m => m.get(partition))
          .getOrElse(addr)
      }
  }
}

object PartitionBrokerLookup {

  type BrokerAddress = String
  type PartitionToBrokerMap = Map[NamedPartition, BrokerAddress]

  private val log = getLogger(this)

  val WriteBrokerLookupProperty = "optimus.writepartitions.partition"
  val PubSubBrokerLookupProperty = "optimus.pubsubpartitions"

  def apply(config: RuntimeConfiguration): PartitionBrokerLookup = {
    val writeBrokerMap = getBrokerLookupMap(config, WriteBrokerLookupProperty)
    log.info(s"Partition to write broker lookup map $writeBrokerMap")
    val pubsubBrokerMap = getPubSubBrokerLookupMap(config)
    log.info(s"Partition to pubsub broker lookup map $pubsubBrokerMap")
    PartitionBrokerLookup(writeBrokerMap, pubsubBrokerMap)
  }

  private def getBrokerLookupMap(config: RuntimeConfiguration, xmlPath: String): PartitionToBrokerMap = {
    val partitionToLookupProps =
      config.getStringListWithAttributes(xmlPath)
    val partitionToLookupAttributes = partitionToLookupProps.map(_._2)
    val partitionToBrokerLookupMap =
      partitionToLookupAttributes.map(m => NamedPartition(m("name")) -> m("lookup")).toMap
    partitionToBrokerLookupMap
  }

  private def getPubSubBrokerLookupMap(config: RuntimeConfiguration): Map[BrokerAddress, PartitionToBrokerMap] = {
    val regionalAddresses = config.getProperties(PartitionBrokerLookup.PubSubBrokerLookupProperty)
    if (regionalAddresses.size != regionalAddresses.toSet.size)
      throw new OptimusConfigurationException("More than one tag for some regional address(s)!")
    regionalAddresses
      .map { regionalAddressList =>
        regionalAddressList map { address =>
          val regionalAddressXmlPath = s"${PartitionBrokerLookup.PubSubBrokerLookupProperty}.$address.partition"
          val partitionToBrokerLookupMap = getBrokerLookupMap(config, regionalAddressXmlPath)
          address -> partitionToBrokerLookupMap
        } toMap
      }
      .getOrElse(Map.empty)
  }
}
