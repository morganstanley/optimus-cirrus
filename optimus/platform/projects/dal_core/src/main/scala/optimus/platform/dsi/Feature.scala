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
package optimus.platform.dsi

import optimus.dsi.base.RegisteredIndexConfig
import optimus.graph.DiagnosticSettings
import optimus.platform.internal.SimpleGlobalStateHolder
import optimus.platform.internal.SimpleStateHolder
import optimus.platform.storable.SerializedEntity

import scala.collection.immutable.SortedSet

sealed abstract class Feature(val value: Feature.Id) {
  def prop = s"optimus.dsi.feature.${this.getClass.getSimpleName.stripSuffix("$")}"
  def defaultEnabled: Boolean = true
  def enabled: Boolean = DiagnosticSettings.getBoolProperty(prop, defaultEnabled)
  def enable(): Unit = System.setProperty(prop, "true")
  def disable(): Unit = System.setProperty(prop, "false")
  def clear(): Unit = System.clearProperty(prop)
}

object Feature {
  type Id = Int

  final case class Unknown(override val value: Id = 0) extends Feature(value)
  case object ExtendedChunkedResponses extends Feature(1)
  case object RangeQuery extends Feature(4)
  case object ParseSessionEstablishmentErrors extends Feature(6)
  case object OutOfOrderReception extends Feature(7)
  case object PropagateEntitlementCheckFailed extends Feature(8)
  case object ValidTimeLineLazyLoadEntity extends Feature(9)
  case object ClasspathRegistration extends Feature(10)
  case object SerializeKeyWithRefFilter extends Feature(11)
  case object BreadcrumbTracking extends Feature(12)
  case object DalActionCheck extends Feature(13)
  case object DalOnBehalf extends Feature(14)
  case object BatchLevelErrorResults extends Feature(15)
  case object NonReaderBrokerException extends Feature(16)
  case object BrokerUriResolutionForNonDefaultContext extends Feature(17)
  case object RangeVersionQuery extends Feature(18)
  // the feature is currently unused
  // TODO(OPTIMUS-18417): use the feature
  case object TypedReferences extends Feature(19)
  case object LeadWriterPartitioning extends Feature(20)
  case object ClientSideAppEventReferenceAssignment extends Feature(21)
  case object EstablishAllRolesWithSession extends Feature(22) {
    override def prop: String = "optimus.dsi.server.enableEstablishAllRolesWithSession"
  }
  case object ExecuteRefQueryWithVersionedRef extends Feature(23)
  case object PartitionedServerTime extends Feature(24)
  case object SupportUniqueIndexViolationException extends Feature(25)
  case object SupportCreateNewSession extends Feature(26) {
    override def prop: String = EnableCreateNewSessionState.defaultEnableCreateNewSession.toString
    override def defaultEnabled: Boolean = EnableCreateNewSessionState.defaultEnableCreateNewSession
    override def enable(): Unit = EnableCreateNewSessionState.enableCreateNewSession
    override def disable(): Unit = EnableCreateNewSessionState.disableCreateNewSession
  }
  case object EmitEntityReferenceStats extends Feature(27) {
    override def prop: String = EntityReferenceStatsCollectorState.enableProp
    override def defaultEnabled: Boolean = EntityReferenceStatsCollectorState.defaultEnabled
    override def enable(): Unit = EntityReferenceStatsCollectorState.enableEntityReferenceStatsCollection()
    override def disable(): Unit = EntityReferenceStatsCollectorState.resetEntityReferenceStatsCollection()
  }
  // Note: This pubsub feature was assigned value 28 before, but we had to rework protocol
  // around it so that value CANNOT be used again.
  case object PubSubChunkedNotifications extends Feature(29)

  /**
   * * LastWitnessedTxTimeOfClient is added as part of late tt assignment changes As part of this feature, client is
   * passing it's own lastWitnessedTxTime with every write request lastWitnessedTxTime is used when we prepared a txn
   * from secondaries metadata sources This is used to identify txn's preparation failure requires retry or not
   */
  case object LastWitnessedTxTimeOfClient extends Feature(30)
  case object PubSubOutOfLineNotificationEntry extends Feature(31)
  case object Lz4Compression extends Feature(value = 32)
  case object DeltaUpdatePriqlApi extends Feature(value = 33)
  case object EstablishSessionInPrcClient extends Feature(34)
  case object GetTemporalSpaceWithFilter extends Feature(value = 35)
  case object GetEventTransactionsApi extends Feature(value = 36)
  case object SilverKingAllReplicasExcluded extends Feature(value = 37)
  case object SamplingQuery extends Feature(value = 38)
  case object ValidTimeLineWithBounds extends Feature(value = 39)
  case object TransactionTimelineWithBounds extends Feature(value = 40)
  case object CountGroupings extends Feature(value = 41)
  case object SupportsRevertOp extends Feature(value = 42) {
    override def prop: String = "optimus.dsi.server.supportsRevertOp"
  }

  /**
   * If server supports registered indexes feature, then it will send the list of entities that supports the feature.
   * Accordingly, PriQL may execute comparison predicates on server-side.
   */
  object RegisteredIndexes {
    def empty: RegisteredIndexes = RegisteredIndexes(Set.empty)
    val value: Integer = 43
  }
  final case class RegisteredIndexes(supportedEntities: Set[SerializedEntity.TypeRef])
      extends Feature(value = RegisteredIndexes.value) {
    def supports(entity: SerializedEntity.TypeRef): Boolean = supportedEntities.contains(entity)
    override def prop: String = RegisteredIndexConfig.EnableProp
    override def defaultEnabled: Boolean = RegisteredIndexConfig.registeredIndexesEnabledDefaultValue
  }

  case object AtNowMaxCatchup extends Feature(value = 44)
  case object EventEntitiesWithType extends Feature(value = 45)
  case object AsyncPartitionedWrite extends Feature(value = 46)
  case object SetStreamsACLs extends Feature(value = 47)

  // With this feature, the client will send SK-based filter conditions to server if possible.
  // The server will use Mongodb to execute the SK-based conditions if the related AccInfo has
  // enableSerializedKeyBasedFilter == Some(true), otherwise, it will rewrite the SK-based
  // conditions back to normal acc conditions and use Postgres to execute it.
  case object SerializedKeyBasedFilterForAccelerator extends Feature(value = 48)

  case object RoleMembershipQueryWithClientSessionInfo extends Feature(value = 49)

  case object SupportMonoTemporal extends Feature(value = 50)

  case object PubSubReferenceNotification extends Feature(value = 51) { override def defaultEnabled = false }

  case object VersionedReferenceQuery extends Feature(value = 52)

  def fromValue(value: Id, registeredEntities: Set[SerializedEntity.TypeRef] = Set.empty): Feature = value match {
    case 1  => ExtendedChunkedResponses
    case 4  => RangeQuery
    case 6  => ParseSessionEstablishmentErrors
    case 7  => OutOfOrderReception
    case 8  => PropagateEntitlementCheckFailed
    case 9  => ValidTimeLineLazyLoadEntity
    case 10 => ClasspathRegistration
    case 11 => SerializeKeyWithRefFilter
    case 12 => BreadcrumbTracking
    case 13 => DalActionCheck
    case 14 => DalOnBehalf
    case 15 => BatchLevelErrorResults
    case 16 => NonReaderBrokerException
    case 17 => BrokerUriResolutionForNonDefaultContext
    case 18 => RangeVersionQuery
    case 19 => TypedReferences
    case 20 => LeadWriterPartitioning
    case 21 => ClientSideAppEventReferenceAssignment
    case 22 => EstablishAllRolesWithSession
    case 23 => ExecuteRefQueryWithVersionedRef
    case 24 => PartitionedServerTime
    case 25 => SupportUniqueIndexViolationException
    case 26 => SupportCreateNewSession
    case 27 => EmitEntityReferenceStats
    case 29 => PubSubChunkedNotifications
    case 30 => LastWitnessedTxTimeOfClient
    case 31 => PubSubOutOfLineNotificationEntry
    case 32 => Lz4Compression
    case 33 => DeltaUpdatePriqlApi
    case 34 => EstablishSessionInPrcClient
    case 35 => GetTemporalSpaceWithFilter
    case 36 => GetEventTransactionsApi
    case 37 => SilverKingAllReplicasExcluded
    case 38 => SamplingQuery
    case 39 => ValidTimeLineWithBounds
    case 40 => TransactionTimelineWithBounds
    case 41 => CountGroupings
    case 42 => SupportsRevertOp
    case 43 => RegisteredIndexes(registeredEntities)
    case 44 => AtNowMaxCatchup
    case 45 => EventEntitiesWithType
    case 46 => AsyncPartitionedWrite
    case 47 => SetStreamsACLs
    case 48 => SerializedKeyBasedFilterForAccelerator
    case 49 => RoleMembershipQueryWithClientSessionInfo
    case 50 => SupportMonoTemporal
    case 51 => PubSubReferenceNotification
    case 52 => VersionedReferenceQuery
    case _  => Unknown(value)
  }

  implicit val ordering: Ordering[Feature] = Ordering.by(_.value)
}

final case class SupportedFeatures(features: SortedSet[Feature]) {

  val registeredIndexes: Option[Feature.RegisteredIndexes] = features.collectFirst {
    case r: Feature.RegisteredIndexes => r
  }

  def supports(feature: Feature): Boolean = features contains feature
  def intersect(other: SupportedFeatures): SupportedFeatures = SupportedFeatures(features.intersect(other.features))
  def diff(other: SupportedFeatures): SupportedFeatures = SupportedFeatures(features.diff(other.features))
  def enabled: SupportedFeatures = SupportedFeatures(features.filter(_.enabled))
}

object SupportedFeatures extends SimpleGlobalStateHolder(() => new SupportedFeaturesConfigValues) {
  def apply(features: Feature*): SupportedFeatures = apply(SortedSet.empty[Feature] ++ features)
  def apply(features: Iterable[Feature]): SupportedFeatures = apply(SortedSet.empty[Feature] ++ features)
  def apply(): SupportedFeatures = apply(SortedSet.empty[Feature])

  // TODO (OPTIMUS-11744): remove this method once all brokers support connection-level sessions.
  // A connection-level session already has 'native' support for this new feature negotiation style, so once
  // that propagates the clientProtoFileVersionNum won't be necessary any more. However we can't actually get rid of it
  // until we can move clients away from older versions of the codebase which don't rely on having this.
  def fromProtoFileVersionNum(protoFileVersionNum: Int): SupportedFeatures = {
    require(protoFileVersionNum <= 2)
    if (protoFileVersionNum == 2) All
    else None
  }

  // lazy to avoid initialization before it's needed
  def All: SupportedFeatures = FeatureSets.All
  lazy val None: SupportedFeatures = FeatureSets.None

  def setEnableSyncOutOfOrderReception(value: Boolean): Unit = getState.synchronized {
    getState.setEnableOutOfOrderReception(value)
  }
  def enableSyncOutOfOrderReception: Boolean = getState.enableOutOfOrderReception()

  def setAsyncClient(value: Boolean): Unit = getState.synchronized {
    getState.setAsyncClient(value)
  }
  def asyncClient: Boolean = getState.asyncClient()

  def myFeatures: SupportedFeatures = getState.supportedFeatures()
}

class EnableCreateNewSessionState {
  import EnableCreateNewSessionState._
  @volatile private var createNewSession = defaultEnableCreateNewSession
}

object EnableCreateNewSessionState extends SimpleStateHolder(() => new EnableCreateNewSessionState) {
  val defaultEnableCreateNewSession =
    DiagnosticSettings.getBoolProperty("optimus.dsi.server.enableCreateNewSession", true)

  def enableCreateNewSession: Unit = getState.createNewSession = true

  def disableCreateNewSession: Unit = getState.createNewSession = false

  def reset: Unit = getState.createNewSession = defaultEnableCreateNewSession

  def isCreateNewSessionEnabled = getState.createNewSession
}

class EntityReferenceStatsCollectorState {
  import EntityReferenceStatsCollectorState._

  @volatile private var emitEntityReferenceStatsEnabled = defaultEnableEmitEntityReferenceStats
}

object EntityReferenceStatsCollectorState extends SimpleStateHolder(() => new EntityReferenceStatsCollectorState) {
  private[dsi] val enableProp = "optimus.dsi.enableEmitEntityReferenceStats"
  private[dsi] val defaultEnabled = false
  private val defaultEnableEmitEntityReferenceStats = DiagnosticSettings.getBoolProperty(enableProp, defaultEnabled)

  def enableEntityReferenceStatsCollection(): Unit = getState.emitEntityReferenceStatsEnabled = true

  def resetEntityReferenceStatsCollection(): Unit =
    getState.emitEntityReferenceStatsEnabled = defaultEnableEmitEntityReferenceStats

  def isEntityReferenceStatsCollectionEnabled: Boolean = getState.emitEntityReferenceStatsEnabled
}

private object FeatureSets {

  // avoid class loading on startup
  private lazy val allFeatures = SupportedFeatures(
    Set(
      Feature.ExtendedChunkedResponses,
      Feature.RangeQuery,
      Feature.RangeVersionQuery,
      Feature.ParseSessionEstablishmentErrors,
      Feature.OutOfOrderReception,
      Feature.PropagateEntitlementCheckFailed,
      Feature.ClasspathRegistration,
      Feature.ValidTimeLineLazyLoadEntity,
      Feature.SerializeKeyWithRefFilter,
      Feature.BreadcrumbTracking,
      Feature.DalActionCheck,
      Feature.BatchLevelErrorResults,
      Feature.DalOnBehalf,
      Feature.NonReaderBrokerException,
      Feature.BrokerUriResolutionForNonDefaultContext,
      Feature.TypedReferences,
      Feature.LeadWriterPartitioning,
      Feature.ClientSideAppEventReferenceAssignment,
      Feature.PartitionedServerTime,
      Feature.SupportUniqueIndexViolationException,
      Feature.PubSubChunkedNotifications,
      Feature.LastWitnessedTxTimeOfClient,
      Feature.PubSubOutOfLineNotificationEntry,
      Feature.Lz4Compression,
      Feature.DeltaUpdatePriqlApi,
      Feature.EstablishSessionInPrcClient,
      Feature.GetTemporalSpaceWithFilter,
      Feature.GetEventTransactionsApi,
      Feature.SilverKingAllReplicasExcluded,
      Feature.SamplingQuery,
      Feature.ValidTimeLineWithBounds,
      Feature.TransactionTimelineWithBounds,
      Feature.CountGroupings,
      Feature.AtNowMaxCatchup,
      Feature.AsyncPartitionedWrite,
      Feature.SetStreamsACLs,
      Feature.RoleMembershipQueryWithClientSessionInfo,
      Feature.EstablishAllRolesWithSession,
      Feature.ExecuteRefQueryWithVersionedRef,
      Feature.RegisteredIndexes.empty,
      Feature.SupportCreateNewSession,
      Feature.EmitEntityReferenceStats,
      Feature.EventEntitiesWithType,
      Feature.SerializedKeyBasedFilterForAccelerator,
      Feature.SupportsRevertOp,
      Feature.SupportMonoTemporal,
      Feature.PubSubReferenceNotification,
      Feature.VersionedReferenceQuery
    )
  )

  def All: SupportedFeatures = {
    val allEnabled = allFeatures.enabled
    validate(allEnabled)
    allEnabled
  }

  val None: SupportedFeatures = SupportedFeatures()

  private def validate(supportedFeatures: SupportedFeatures): Unit = {
    // Feature IDs get deny-listed when they are retired from use. In order to maintain backwards compatibility we have to
    // be sure that we're not recycling old IDs for new features. As such we place those IDs in here.
    val denyListedFeatureIds: Set[Feature.Id] = Set(
      2 // Used to be ServerSideReferenceAssignment
    )
    // To check that the All set is valid, we want to ensure that there are no duplicate IDs in use and no deny-listed
    // IDs in use.
    val featuresById: Map[Feature.Id, Set[Feature]] = supportedFeatures.features.groupBy(_.value)
    val duplicatedIds = featuresById.filter { case (_, features) => features.size > 1 }
    val denyListedFeatures = featuresById.filter(x => denyListedFeatureIds.contains(x._1))
    if (duplicatedIds.nonEmpty || denyListedFeatures.nonEmpty) {
      throw new IllegalStateException(
        s"The following duplicated/deny-listed feature IDs are in FeatureSets.All: $duplicatedIds/$denyListedFeatures")
    }
  }
}

final class SupportedFeaturesConfigValues() {
  private def allSupportedFeatures = FeatureSets.All
  private lazy val syncSupportedFeatures = FeatureSets.All.diff(SupportedFeatures(Feature.OutOfOrderReception))
  private var features: SupportedFeatures = _

  private class SyncAsyncSwitch(propertyName: String, defaultValue: Boolean) {
    private var value = DiagnosticSettings.getBoolProperty(propertyName, defaultValue)
    def set(newValue: Boolean): Unit = {
      if (newValue) features = allSupportedFeatures
      else features = syncSupportedFeatures
      value = newValue
    }

    def get(): Boolean = value
  }

  private val enableOutOfOrderReceptionSwitch = new SyncAsyncSwitch("optimus.dsi.enableSyncOutOfOrderReception", false)
  private val asyncClientSwitch = new SyncAsyncSwitch("optimus.dsi.asyncClient", false)

  // Allow forcing certain features to be off at runtime
  private val forceDisableFeatures: SupportedFeatures = Option(System.getProperty("optimus.dsi.forceDisableFeatures"))
    .filter(_.nonEmpty)
    .map(k => {
      SupportedFeatures(
        k.split(',')
          .map(x => {
            Feature.fromValue(x.trim.toInt)
          }))
    })
    .getOrElse(FeatureSets.None)

  def setEnableOutOfOrderReception(value: Boolean): Unit = enableOutOfOrderReceptionSwitch.set(value)
  def enableOutOfOrderReception(): Boolean = enableOutOfOrderReceptionSwitch.get()

  def setAsyncClient(value: Boolean): Unit = asyncClientSwitch.set(value)
  def asyncClient(): Boolean = asyncClientSwitch.get()

  def supportedFeatures(): SupportedFeatures = {
    if (features eq null)
      features = allSupportedFeatures
    features.diff(forceDisableFeatures)
  }
}
