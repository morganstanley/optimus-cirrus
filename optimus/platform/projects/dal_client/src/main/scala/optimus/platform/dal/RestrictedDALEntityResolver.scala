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

import java.util

import java.time.Instant
import optimus.dsi.session.EstablishedClientSession
import optimus.entity.IndexInfo
import optimus.platform._
import optimus.platform.dal
import optimus.platform.dsi.bitemporal.DSI
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.Key

object RestrictedEntityResolverException
    extends RuntimeException("not allowed to do dal call within withDalRoles block")

@entity class RestrictedEntityResolver(
    rootDsi: DSI,
    val ecs: EstablishedClientSession,
    private[optimus] val unrestricted: EntityResolver)
    extends EntityResolver
    with SessionFetcher
    with HasPartitionMap {
  protected[optimus] final def dsi = rootDsi
  protected[optimus] final def partitionMap = dsi.partitionMap

  require(
    !unrestricted.isInstanceOf[RestrictedEntityResolver],
    s"unrestricted entity resolver cannot be of type ${RestrictedEntityResolver.getClass.getName}")

  @node @scenarioIndependent private[optimus] def getSession(tryEstablish: Boolean = true): EstablishedClientSession =
    ecs

  override private[optimus] def initGlobalSessionEstablishmentTime(time: Instant) =
    throw new IllegalArgumentException("cannot re-assign global sessionEstablishmentTime for RestrictedEntityResolver")

  override private[optimus] def forceGlobalSessionEstablishmentTimeTo(time: Instant): Option[Instant] =
    throw new IllegalArgumentException("cannot change global sessionEstablishmentTime for RestrictedEntityResolver")

  override private[optimus] def getGlobalSessionEstablishmentTime = Some(ecs.establishmentTime)

  // TODO (OPTIMUS-23546): please correct the exception and message
  @node @scenarioIndependent private def throwException =
    throw RestrictedEntityResolverException

  @node @scenarioIndependent def findEntity[S <: Entity](key: Key[S], temporality: TemporalContext) = throwException
  @node @scenarioIndependent def findEntityOption[S <: Entity](key: Key[S], temporality: TemporalContext) =
    throwException
  @node @scenarioIndependent def findByReference(eRef: EntityReference, temporality: TemporalContext) = throwException
  @node @scenarioIndependent def findByIndex[E <: Entity](
      idx: Key[E],
      temporality: TemporalContext,
      classInfo: Option[Class[E]],
      entitledOnly: Boolean) =
    throwException
  @node @scenarioIndependent def findByIndexInRange[E <: Entity](
      key: Key[E],
      fromTemporalContext: TemporalContext,
      toTemporalContext: TemporalContext) = throwException
  @node @scenarioIndependent def findByIndexWithEref[E <: Entity](
      idx: Key[E],
      erefs: Iterable[EntityReference],
      temporality: TemporalContext) = throwException
  @node @scenarioIndependent def enumerateKeysWithRtt[E <: Entity](
      indexInfo: IndexInfo[E, _],
      when: QueryTemporality,
      rtt: Instant) =
    throwException
  @node @scenarioIndependent private[optimus] def getPersistentEntityByRef(
      eRef: EntityReference,
      temporality: TemporalContext) =
    throwException
  @node @scenarioIndependent private[optimus] /*[platform]*/ def enumerateQuery[E <: Entity](
      query: ElementQuery,
      classNames: Seq[String],
      temporality: TemporalContext) = throwException
  @node @scenarioIndependent private[optimus] /*[platform]*/ def enumerateReferenceQuery[E <: Entity](
      query: ElementQuery,
      classNames: Seq[String],
      loadContext: TemporalContext) = throwException

  def translateMarkers(markers: util.IdentityHashMap[dal.Marker[_], Tweak]) = throwException
  protected[optimus] /*[platform]*/ def serverInfo = throwException
  def cacheManager = throwException
  protected[optimus] /*[platform]*/ def serverTime = throwException

  // it's always safe to return false
  override def equivalentTo(other: EntityResolver): Boolean = false
}
