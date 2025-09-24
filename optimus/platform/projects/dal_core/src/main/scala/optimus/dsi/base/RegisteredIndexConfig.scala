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
package optimus.dsi.base

import optimus.dsi.partitioning.Partition
import optimus.dsi.partitioning.PartitionHelper
import optimus.graph.DiagnosticSettings
import optimus.platform.internal.SimpleStateHolder

import java.util.concurrent.atomic.AtomicReference
import java.util.regex.Pattern

class RegisteredIndexConfigProperties {
  import RegisteredIndexConfig._

  val unregisteredIndexWriteNotAllowedInPartitions: AtomicReference[Set[Partition]] =
    new AtomicReference(
      stringToPartition(DiagnosticSettings.getStringProperty(UnregisteredIndexNotAllowedInPartitionsProp, "")))

  private def stringToPartition(partitions: String): Set[Partition] = {
    partitions
      .split(",")
      .map(_.trim)
      .map(PartitionHelper.getPartitionForString)
      .toSet
  }

  def setUnregisteredIndexWriteNotAllowedInPartitions(partitions: String): Unit =
    unregisteredIndexWriteNotAllowedInPartitions.set(stringToPartition(partitions))
}

private[optimus] object RegisteredIndexConfig extends SimpleStateHolder(() => new RegisteredIndexConfigProperties()) {

  val regIndexWriteCrumbStr = "RegisteredIndexWrite"
  val EnableProp: String = "optimus.dal.registered.indexes.enable"
  private val EntitiesListCompressionTypeProp: String =
    "optimus.dal.registered.indexes.entitiesListCompressionType"
  private val MaxStringLengthProp: String = "optimus.dal.registered.indexes.writer.maxStringLength"
  val UnregisteredIndexNotAllowedInPartitionsProp: String =
    "optimus.dal.registered.indexes.unregisteredIndexNotAllowedInPartitions"
  val EnableForceBrokerCacheUpdateForRegressionAndPerfTests: String =
    "optimus.dal.registered.indexes.enableForceBrokerCacheUpdateForRegressionAndPerfTests"
  private val DisableSingleBackendQueryExecution: String =
    "optimus.dal.registered.indexes.disableSingleBackendQueryExecution"

  // DO NOT USE this in real deployment. This is only for autosys jobs, integration & performance tests.
  val ScriptOrTestOnlyNonInteractiveModeProp =
    "optimus.dal.registered.indexes.manager.scriptOrtestOnlyNonInteractiveMode"

  // This allow-list is to avoid "bootstrap" problem with registration, which may also write some entities
  // with index fields.
  // Change back to full classname match after entity migration is complete.
  private val oldIndexRegistrationModeAllowList: Set[Pattern] = Set(
    Pattern.compile("optimus\\.[a-z]+\\.session\\.SlotMetadata"))

  def isOldIndexRegistrationModeAllowed(className: String): Boolean =
    oldIndexRegistrationModeAllowList.exists(pattern => pattern.matcher(className).matches())

  def registeredIndexesEnabledDefaultValue: Boolean = true
  def areRegisteredIndexesEnabled: Boolean =
    DiagnosticSettings.getBoolProperty(
      EnableProp,
      registeredIndexesEnabledDefaultValue
    )

  def isForceBrokerCacheUpdateForRegressionAndPerfTestsEnabled: Boolean =
    DiagnosticSettings.getBoolProperty(EnableForceBrokerCacheUpdateForRegressionAndPerfTests, false)

  def registeredEntitiesListCompressionType: String =
    DiagnosticSettings.getStringProperty(EntitiesListCompressionTypeProp, "LZ4")

  val permissibleMaxStringLength: Int =
    DiagnosticSettings.getIntProperty(MaxStringLengthProp, 4096)

  // This flag would be useful where we have a brand new partition and we only allow writing
  // registered indexes in that partition.
  def isUnregisteredIndexWriteNotAllowedInPartition(partition: Partition): Boolean =
    getState.unregisteredIndexWriteNotAllowedInPartitions.get.contains(partition)

  def setUnregisteredIndexWriteNotAllowedInPartition(partitions: String): Unit =
    getState.setUnregisteredIndexWriteNotAllowedInPartitions(partitions)

  // In first version of registered indexes, we only support the case where all @indexed vals are defined in the entity
  // being registered so we will have registration time check.
  // However, we do have partial implementation to support the full entity class hierarchy where some @indexed vals may
  // belong in base classes/traits. And we have test cases as well. So, this flag **should be only disabled in tests**.
  val BaseTypeWithIndexFieldShouldFailRegistrationPropName =
    "optimus.dal.registered.indexes.baseTypeWithIndexFieldShouldFailRegistration"
  def baseTypeWithIndexFieldShouldFailRegistration: Boolean =
    DiagnosticSettings.getBoolProperty(BaseTypeWithIndexFieldShouldFailRegistrationPropName, false)

  def disableSingleBackendQueryExecution: Boolean =
    DiagnosticSettings.getBoolProperty(DisableSingleBackendQueryExecution, false)

  val serializedKeysMaxSizeBytes: Int =
    DiagnosticSettings.getIntProperty("optimus.dal.registered.indexes.serializedKeysMaxSizeBytes", 6 * 1024 * 1024)
}
