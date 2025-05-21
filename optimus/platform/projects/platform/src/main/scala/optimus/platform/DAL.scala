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
package optimus.platform

import msjava.base.util.uuid.MSUuid
import optimus.core.needsPlugin
import optimus.graph.NodeKey
import optimus.graph.PluginSupport
import optimus.platform.annotations.nodeLift
import optimus.platform.dal.EntityEventModule.EntityOps
import optimus.platform.storable._
import optimus.platform.dal._
import optimus.platform.internal.CopyMethodMacros
import optimus.platform.storable.EntityChange
import optimus.platform.temporalSurface.EntityTemporalInformation
import optimus.platform.temporalSurface.advanced.TemporalContextUtils

import scala.annotation.compileTimeOnly

/**
 * Implements entity operations by going directly against the entity store (no caching)
 */
object DAL
    extends DALModuleAPI
    with DALUnsafe
    with DALDebug
    with DALAudit
    with DALEntitlements
    with DALLegacy
    with DALSchemas
    with DALAccelerator
    with DALClientSession {
  private[optimus] def obliterateEntityByClassName(className: String): Unit =
    resolver.obliterateEntityByClassName(className)

  def obliterateClass[E <: Entity: Manifest]() = resolver.obliterateClass[E]()

  def obliterateEntity(key: Key[_]) = resolver.obliterateEntity(key)

  def obliterateEntity(ref: EntityReference) = resolver.obliterateEntities(Seq(ref))

  def obliterateEntities(refs: Seq[EntityReference], classNameOpt: Option[String]): Unit =
    resolver.obliterateEntities(refs, classNameOpt)

  def obliterateEntityForClass[E <: Entity: Manifest](ref: EntityReference) = {
    val className = manifest[E].runtimeClass.getName
    resolver.obliterateEntityForClass(ref, className)
  }

  def obliterateEventClass[B <: BusinessEvent: Manifest] = resolver.obliterateEventClass[B]()

  def obliterateEvent(ref: BusinessEventReference) = resolver.obliterateEvent(ref)

  def obliterateEvents(ref: Seq[BusinessEventReference], classNameOpt: Option[String]) =
    resolver.obliterateEvents(ref, classNameOpt)

  def obliterateEventForClass(ref: BusinessEventReference, className: String) =
    resolver.obliterateEventForClass(ref, className)

  def obliterateEvent(key: Key[_]) = resolver.obliterateEvent(key)

  /**
   * DAL-level read APIs
   */
  @node
  def resolveAtOption[E <: Entity](e: E, tc: TemporalContext = loadContext): Option[E] = {
    val ref = e.dal$entityRef
    assert(
      ref != null,
      "This query requires the entity instance to be a stored entity. It cannot be called with an heap entity as argument: " + e
        .getClass()
        .getCanonicalName() + " " + e.coreGetProps()
    )
    val res = resolver.findByReferenceOption(ref, tc).asInstanceOf[Option[E]]
    res
  }

  @node
  def resolveAt[E <: Entity](e: E, tc: TemporalContext = loadContext) = {
    resolveAtOption(e, tc) getOrElse { throw new EntityNotFoundByRefException(e.dal$entityRef, tc) }
  }

  override def resolver: DALEntityResolver =
    if (EvaluationContext.entityResolver.isInstanceOf[RestrictedEntityResolver])
      throw RestrictedEntityResolverException
    else
      EvaluationContext.entityResolver.asInstanceOf[DALEntityResolver]

  def backReference[A <: Entity](f: A => Entity): Iterable[A] = macro DALMacros.backReferenceImpl[A]

  object Modify extends Dynamic {
    def applyDynamic(name: String)(args: Any*): Unit = macro CopyMethodMacros.dalModify0Impl
    def applyDynamicNamed(name: String)(args: (Any, Any)*): Unit = macro CopyMethodMacros.dalModifyImpl
  }

  @compileTimeOnly("missing second arglist for DAL.modify")
  def modify(x: Entity) = Modify

  @node def getEntityOptionByCmid[E <: Entity: Manifest](cmid: MSUuid): Option[E] = {
    val klass = implicitly[Manifest[E]].runtimeClass.asInstanceOf[Class[E]]
    val entityTemporalInfo = TemporalContextUtils
      .loadTemporalCoordinatesForEntityClass(klass, loadContext)
      .asInstanceOf[EntityTemporalInformation]
    val at = QueryTemporality.At(entityTemporalInfo.vt, entityTemporalInfo.tt)
    val pe = resolver.getByCmReferenceWithTemporalityAt(klass, CmReference(cmid.asBytes), at)
    pe.headOption.map(resolver.deserializeOne(loadContext)(_).asInstanceOf[E])
  }

  @node def getEntityByCmid[E <: Entity: Manifest](cmid: MSUuid): E = {
    val entityOpt = getEntityOptionByCmid[E](cmid)
    entityOpt.getOrElse(throw new EntityNotFoundByCmidException(cmid, loadContext))
  }

  // TODO (OPTIMUS-7364): can be public once removal of SelectBusinessEventResult reaches all envs
  @node private[optimus] def getBusinessEventOptionByCmid[E <: BusinessEvent: Manifest](cmid: MSUuid): Option[E] = {
    val klass = implicitly[Manifest[E]].runtimeClass.asInstanceOf[Class[E]]
    val evt = resolver.getBusinessEventByCmReference(klass, CmReference(cmid.asBytes), loadContext)
    evt.map(_.asInstanceOf[E])
  }

  // TODO (OPTIMUS-7364): can be public once removal of SelectBusinessEventResult reaches all envs
  @node private[optimus] def getBusinessEventByCmid[E <: BusinessEvent: Manifest](cmid: MSUuid): E = {
    val evtOpt = getBusinessEventOptionByCmid[E](cmid)
    evtOpt.getOrElse(throw new EventNotFoundByCmidException(cmid, loadContext.ttContext.unsafeTxTime))
  }

  // TODO (OPTIMUS-46212): cleanup usages of it and remove it
  @node
  def getEntityVersionsSince[E <: Entity: Manifest](
      fromTemporalContext: TemporalContext,
      appId: Option[String] = None,
      userId: Option[String] = None): Seq[EntityVersionHolder[E]] = {
    val rangeQueryOpts = RangeQueryOptions(appId = appId, userId = userId)
    val typeName = implicitly[Manifest[E]].runtimeClass.asInstanceOf[Class[E]].getName
    EntityOps.getEntityVersionsInRangeWithoutManifest(fromTemporalContext, loadContext, typeName, rangeQueryOpts)
  }

  // TODO (OPTIMUS-46212): cleanup usages of it and remove it
  @node
  def getEntityChangesInRange[E <: Entity: Manifest](
      fromTemporalContext: TemporalContext,
      appId: Option[String] = None,
      userId: Option[String] = None): Seq[Seq[EntityChange[E]]] = {
    val rangeQueryOpts = RangeQueryOptions(appId, userId, inRange = true)
    val typeName = implicitly[Manifest[E]].runtimeClass.asInstanceOf[Class[E]].getName
    EntityOps.getEntityChangesWithoutManifest(fromTemporalContext, loadContext, typeName, rangeQueryOpts)
  }

  @nodeLift
  def referenceHolderOf[T <: Entity](entity: T): EntityReferenceHolder[T] = needsPlugin
  def referenceHolderOf$node[T <: Entity](entity: NodeKey[T]): EntityReferenceHolder[T] = {
    val result = PluginSupport.getEntityReferenceHolders[T](entity)
    require(result.size == 1, "Expected one result of type EntityReferenceHolder, but got: $result")
    result.head
  }

  @nodeLift
  def referenceHoldersOf[T <: Entity](entities: Iterable[T]): Iterable[EntityReferenceHolder[T]] = needsPlugin
  def referenceHoldersOf$node[T <: Entity](entities: NodeKey[Iterable[T]]): Iterable[EntityReferenceHolder[T]] =
    PluginSupport.getEntityReferenceHolders[T](entities)
}
