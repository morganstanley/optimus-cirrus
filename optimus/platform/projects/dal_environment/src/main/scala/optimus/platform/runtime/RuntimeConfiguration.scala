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
package optimus.platform.runtime

import optimus.breadcrumbs.ChainedID
import optimus.config.RuntimeConfiguration
import optimus.dsi.partitioning.PartitionMap
import optimus.platform.dal.RuntimeProperties
import optimus.platform.dal.config.DalAppId

import java.time.Instant

class MockRuntimeConfiguration(
    val mode: String = "mock",
    appId: DalAppId = DalAppId("MockRuntimeConfiguration"),
    partitionMap: PartitionMap = PartitionMap()
) extends RuntimeConfiguration {

  override val rootID = ChainedID.create()

  override def specificPropertyMap =
    Map(RuntimeProperties.DsiUriProperty -> "memory://mock", RuntimeProperties.DsiAppIdProperty -> appId.underlying)

  override def env: String = mode
  override def instance: Option[String] = None
  override def getBoolean(name: String): Option[Boolean] = None
  override def getInt(name: String): Option[Int] = None
  override def getString(name: String): Option[String] = propertyMap.get(name)
  override def getStringList(name: String): Option[List[String]] = None
  override def getByteVector(name: String): Option[Vector[Byte]] = None
  override def getProperties(baseName: String): Option[List[String]] = None
  override def getAllProperties(baseName: String): Option[Set[String]] = None
  override def get(name: String): Option[Any] =
    if (name == PartitionMap.PartitionMapProperty) Some(partitionMap) else None
  override def getInstant(name: String): Option[Instant] = None
  override def getAttributes(name: String): Map[String, String] = Map.empty
  override def getStringListWithAttributes(name: String): List[(String, Map[String, String])] = List.empty

  override def withOverride(name: String, value: Any) = new ConfigOverrider(this, Map(name -> value))
  override def withOverrides(m: Map[String, Any]) = new ConfigOverrider(this, m)
}
