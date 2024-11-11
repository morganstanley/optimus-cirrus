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
package optimus.platform.dal

import java.time.Instant
import optimus.dsi.partitioning.Partition
import optimus.entity._
import optimus.graph.NodeKey
import optimus.graph.NodeFuture
import optimus.platform.annotations.nodeSync
import optimus.platform.internal.ClassInfo
import optimus.platform.relational.tree.MultiRelationElement
import optimus.platform.storable._
import optimus.platform.temporalSurface.TemporalSurfaceCacheManager
import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery
import optimus.platform.TemporalContext
import optimus.platform.Tweak
import optimus.platform.entersGraph
import optimus.platform.scenarioIndependent

protected[optimus] /*[platform]*/ trait ServerInfo {
  def build: Int
  def infoTag: String
  def isDefined: Boolean
  def daysDiff(that: ServerInfo): Option[Long]
}

trait EntityResolver {

  /**
   * Finds an entity with the given key valid during the temporal context.
   */
  // TODO  - make to UniqueKey, not Key
  @nodeSync
  @scenarioIndependent
  def findEntity[S <: Entity](key: Key[S], temporality: TemporalContext): S
  def findEntity$queued[S <: Entity](key: Key[S], temporality: TemporalContext): NodeFuture[S]
  def findEntity$newNode[S <: Entity](
      key: Key[S],
      temporality: TemporalContext)
      : NodeKey[S] // TODO (OPTIMUS-0000): Remove all xxx$newNode they are meaningless here!

  @nodeSync
  @scenarioIndependent
  def findEntityOption[S <: Entity](key: Key[S], temporality: TemporalContext): Option[S]
  def findEntityOption$queued[S <: Entity](key: Key[S], temporality: TemporalContext): NodeFuture[Option[S]]
  def findEntityOption$newNode[S <: Entity](key: Key[S], temporality: TemporalContext): NodeKey[Option[S]]

  @nodeSync
  @scenarioIndependent
  def findByReference(eRef: EntityReference, temporality: TemporalContext): Entity
  def findByReference$queued(eRef: EntityReference, temporality: TemporalContext): NodeFuture[Entity]
  def findByReference$newNode(eRef: EntityReference, temporality: TemporalContext): NodeKey[Entity]

  @nodeSync
  @scenarioIndependent
  def findByIndex[E <: Entity](
      idx: Key[E],
      temporality: TemporalContext,
      classInfo: Option[Class[E]] = None,
      entitledOnly: Boolean = false): Iterable[E]
  def findByIndex$queued[E <: Entity](
      idx: Key[E],
      temporality: TemporalContext,
      classInfo: Option[Class[E]],
      entitledOnly: Boolean): NodeFuture[Iterable[E]]
  def findByIndex$newNode[E <: Entity](
      idx: Key[E],
      temporality: TemporalContext,
      classInfo: Option[Class[E]],
      entitledOnly: Boolean): NodeKey[Iterable[E]]

  @nodeSync
  @scenarioIndependent
  def findByIndexInRange[E <: Entity](
      key: Key[E],
      fromTemporalContext: TemporalContext,
      toTemporalContext: TemporalContext): Iterable[VersionHolder[E]]
  def findByIndexInRange$queued[E <: Entity](
      key: Key[E],
      fromTemporalContext: TemporalContext,
      toTemporalContext: TemporalContext): NodeFuture[Iterable[VersionHolder[E]]]
  def findByIndexInRange$newNode[E <: Entity](
      key: Key[E],
      fromTemporalContext: TemporalContext,
      toTemporalContext: TemporalContext): NodeKey[Iterable[VersionHolder[E]]]

  @nodeSync
  @scenarioIndependent
  def findByIndexWithEref[E <: Entity](
      idx: Key[E],
      erefs: Iterable[EntityReference],
      temporality: TemporalContext): Iterable[E]
  def findByIndexWithEref$queued[E <: Entity](
      idx: Key[E],
      erefs: Iterable[EntityReference],
      temporality: TemporalContext): NodeFuture[Iterable[E]]
  def findByIndexWithEref$newNode[E <: Entity](
      idx: Key[E],
      erefs: Iterable[EntityReference],
      temporality: TemporalContext): NodeKey[Iterable[E]]

  @nodeSync
  @scenarioIndependent
  def enumerateKeysWithRtt[E <: Entity](
      indexInfo: IndexInfo[E, _],
      when: QueryTemporality,
      rtt: Instant): Iterable[SortedPropertyValues]
  def enumerateKeysWithRtt$queued[E <: Entity](
      indexInfo: IndexInfo[E, _],
      when: QueryTemporality,
      rtt: Instant): NodeFuture[Iterable[SortedPropertyValues]]
  def enumerateKeysWithRtt$newNode[E <: Entity](
      indexInfo: IndexInfo[E, _],
      when: QueryTemporality,
      rtt: Instant): NodeKey[Iterable[SortedPropertyValues]]

  @nodeSync
  @scenarioIndependent
  private[optimus] def getPersistentEntityByRef(
      eRef: EntityReference,
      temporality: TemporalContext): Option[PersistentEntity]
  private[optimus] def getPersistentEntityByRef$queued(
      eRef: EntityReference,
      temporality: TemporalContext): NodeFuture[Option[PersistentEntity]]
  private[optimus] def getPersistentEntityByRef$newNode(
      eRef: EntityReference,
      temporality: TemporalContext): NodeKey[Option[PersistentEntity]]

  def translateMarkers(markers: java.util.IdentityHashMap[optimus.platform.dal.Marker[_], Tweak]): collection.Seq[Tweak]

  protected[optimus] /*[platform]*/ def serverTime: Map[Partition, Instant]

  protected[optimus] /*[platform]*/ def init(config: optimus.config.RuntimeComponents): Unit = {}

  // TODO (OPTIMUS-21169): this path can be removed when all brokers support
  // resolving allRoles on the server-side during session establishment (EstablishAllRolesWithSession)

  // Actually it's not good that the behaviour of EntityResolver can be changed in this way after its construction. But
  // we do need an EvaluationContext to be setup before we change the session parameters of EntityResolver. We have to
  // make it a separate step ofter the initialization.
  @entersGraph
  protected[optimus] /*[platform]*/ def reinitWithLocallyResolvedAllRoles(
      config: optimus.config.RuntimeComponents): Unit = {}

  protected[optimus] /*[platform]*/ def close(): Unit = {}

  @nodeSync
  @scenarioIndependent
  private[optimus] /*[platform]*/ def enumerateQuery[E <: Entity](
      query: ElementQuery,
      classNames: collection.Seq[String],
      temporality: TemporalContext): Iterable[E]
  private[optimus] /*[platform]*/ def enumerateQuery$queued[E <: Entity](
      query: ElementQuery,
      classNames: collection.Seq[String],
      temporality: TemporalContext): NodeFuture[Iterable[E]]
  private[optimus] /*[platform]*/ def enumerateQuery$newNode[E <: Entity](
      query: ElementQuery,
      classNames: collection.Seq[String],
      temporality: TemporalContext): NodeKey[Iterable[E]]

  @nodeSync
  @scenarioIndependent
  // TODO (OPTIMUS-16774): move EntityReferenceHolder to core and fix the signature,and the callers
  private[optimus] /*[platform]*/ def enumerateReferenceQuery[E <: Entity](
      query: ElementQuery,
      classNames: collection.Seq[String],
      loadContext: TemporalContext): Iterable[ReferenceHolder[E]]
  private[optimus] /*[platform]*/ def enumerateReferenceQuery$queued[E <: Entity](
      query: ElementQuery,
      classNames: collection.Seq[String],
      loadContext: TemporalContext): NodeFuture[Iterable[ReferenceHolder[E]]]
  private[optimus] /*[platform]*/ def enumerateReferenceQuery$newNode[E <: Entity](
      query: ElementQuery,
      classNames: collection.Seq[String],
      loadContext: TemporalContext): NodeKey[Iterable[ReferenceHolder[E]]]

  protected[optimus] /*[platform]*/ def serverInfo: ServerInfo

  def cacheManager: TemporalSurfaceCacheManager
}

/**
 * contains callback mechanism for the temporal context to resolve entity and event temporallity
 */
private[optimus] trait TemporalContextEntityResolver {
  @nodeSync
  @scenarioIndependent
  def loadClassInfo(entityRef: EntityReference): ClassInfo
  def loadClassInfo$queued(entityRef: EntityReference): NodeFuture[ClassInfo]

  @nodeSync
  @scenarioIndependent
  def getItemKeys(operation: TemporalSurfaceQuery)(
      sourceTemporality: operation.TemporalityType): collection.Seq[operation.ItemKey]
  def getItemKeys$queued(operation: TemporalSurfaceQuery)(
      sourceTemporality: operation.TemporalityType): NodeFuture[collection.Seq[operation.ItemKey]]

  @nodeSync
  @scenarioIndependent
  def getItemData(operation: TemporalSurfaceQuery)(
      sourceTemporality: operation.TemporalityType,
      itemTemporality: operation.TemporalityType): Map[operation.ItemKey, operation.ItemData]
  def getItemData$queued(operation: TemporalSurfaceQuery)(
      sourceTemporality: operation.TemporalityType,
      itemTemporality: operation.TemporalityType): NodeFuture[Map[operation.ItemKey, operation.ItemData]]

  @nodeSync
  @scenarioIndependent
  def getSingleItemData(operation: TemporalSurfaceQuery)(
      temporality: operation.TemporalityType,
      key: operation.ItemKey): operation.ItemData
  def getSingleItemData$queued(operation: TemporalSurfaceQuery)(
      temporality: operation.TemporalityType,
      key: operation.ItemKey): NodeFuture[operation.ItemData]

}

trait ElementQuery {
  def element: MultiRelationElement
}
