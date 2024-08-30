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
package optimus.platform.dal.servicediscovery

import optimus.dsi.partitioning.Partition

// Supertype to represent types of DAL services
private[optimus] sealed trait DalServiceType
private[optimus] object DalServiceType {
  // Represents a read service, such as a read broker or a PRC server
  private[optimus] case object Read extends DalServiceType
  // Represents a write service (currently just a write broker)
  private[optimus] case object Write extends DalServiceType
  // Represents a DAL PubSub service (currently just a pubsub broker)
  private[optimus] case object PubSub extends DalServiceType
  // Represents a DAL Service Discovery service (currently just a service discovery broker)
  private[optimus] case object ServiceDiscovery extends DalServiceType
  // Represents an accelerated service (current can only be an acc broker)
  private[optimus] case object Accelerated extends DalServiceType
  // Represents a Deriv1 Transformer service which provides server-side versioning
  private[optimus] case object Deriv1Transformer extends DalServiceType
  // Represents a Baikal service which provides datahub proxy address
  private[optimus] case object Baikal extends DalServiceType
  // Represents Messages Service
  private[optimus] case object Messages extends DalServiceType
}

// Supertype to represent providers of DAL services
private[optimus] sealed trait DalServiceProviderType
private[optimus] object DalServiceProviderType {
  // Represents a broker, e.g. a read broker or write broker
  private[optimus] case object Broker extends DalServiceProviderType
  // Represents a PRC server
  private[optimus] case object Prc extends DalServiceProviderType
  // Represents a Deriv1 transformation process
  private[optimus] case object Deriv1TransformationProcess extends DalServiceProviderType
  // Represents a Proxy address service
  private[optimus] case object Proxy extends DalServiceProviderType
}

// Supertype to represent mechanisms for looking up DAL services
private[optimus] sealed trait DalServiceLookupType
private[optimus] object DalServiceLookupType {
  // Represents service lookups through ZooKeeper
  private[optimus] case object ZooKeeper extends DalServiceLookupType
}

// A single discovered DAL service, which can be identified as follows:
// - the serviceType (e.g. Read)
// - the serviceProviderType (e.g. Broker)
// - the lookupType (e.g. ZooKeeper)
// - the lookupStr (e.g. "devln")
// - optionally, the partition (for this example most likely None as read brokers are not partition-aware)
private[optimus] final case class ServiceDiscoveryElement(
    serviceType: DalServiceType,
    serviceProviderType: DalServiceProviderType,
    lookupType: DalServiceLookupType,
    partition: Option[Partition],
    lookupStr: String)
